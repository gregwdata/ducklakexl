import duckdb
import pandas as pd
import os
import asyncio
import string
import requests
import aiohttp


class DuckLakeXL():
    """Wrap DuckDB ducklake functionality in a way that syncs a local copy of the 
    catalog to an Excel file (remote on SharePoint/OneDrive or local)"""

    def __init__(self, 
                 excel_path: str, 
                 data_path: str, 
                 local_catalog: str = 'ducklakexl.ducklake', 
                 duckdb_conn: duckdb.DuckDBPyConnection | None = None,
                 ducklake_name: str = 'my_ducklake',
                 #encrypted: bool = False,
                 custom_cert_store = 'certifi',
                 ):
        """Initialize DuckLakeXL instance.
        Wrap DuckDB ducklake functionality in a way that syncs a local copy of the 
        catalog to an Excel file (remote on SharePoint/OneDrive or local)

        Args:
            excel_path (str): Path to the Excel file to be processed. If a local file, a simple path is used. 
                    If a remove OneDrive file, expects the "resid" query parameter from the URL when you load the file in browser.
                    In the case of OneDrive/Sharepoint, we expect the file already exists.
            data_path (str): Path where the data lake files will be stored
            local_catalog (str, optional): Name of the DuckLake catalog. Defaults to 'ducklakexl.ducklake'
            duckdb_conn (duckdb.DuckDBPyConnection, optional): Existing DuckDB connection. If None, creates new in-memory connection.
                Defaults to None.
        """
                
        self.excel_path = excel_path
        self.data_path = data_path
        self.local_catalog = local_catalog
        self.ducklake_name = ducklake_name
        #self.encrypted = encrypted
        self.custom_cert_store = custom_cert_store
        self._pick_client()
        self._initialize_client()

        # the MSGraph sdk is all async coroutines - for initial simplicity, keep this class all sync
        # whenever we call one of the graph functions, need to wrap in a loop:
        self.loop = asyncio.get_event_loop()

        # if user supplies an existing duckdb connection, use that, otherwise we create a new in-memory one
        if duckdb_conn:
            self.db = duckdb_conn
        else:
            self.db = duckdb.connect()
        
        self._initialize_ducklake()


    def _pick_client(self):
        """based on the excel_path string, decide if we are using a local excel, onedrive, or sharepoint"""
        
        # initial implementation is onedrive only. later, will look at self.excel_path to detect
        self.client_type = 'onedrive'


    def _acquire_token(self):
        """retrieve existing token, if cached. otherwise request new
        Returns a headers dict to pass to request"""
        accounts = self.app.get_accounts(username=None)
        if len(accounts) > 0:
            account = accounts[0]  # Simulate user selection
        else:
            account = None
        result = self.app.acquire_token_silent(self.scopes, account=account)

        if not result:
            result = self.app.acquire_token_interactive(scopes=self.scopes)
            if "access_token" in result:
                print('Authentication successful.')
                access_token = result['access_token']
                # Use the access token to call Microsoft Graph API
                headers = {'Authorization': f'Bearer {access_token}'}
                response = requests.get('https://graph.microsoft.com/v1.0/me', headers=headers)

                if response.status_code == 200:
                    user_data = response.json()
                    self.username = user_data['userPrincipalName']
                else:
                    print(f"API call failed with status code {response.status_code}: {response.text}")
            else:
                print(f"Authentication failed: {result.get('error_description')}")
        else:
            #print("Successfully retrieved token from cache!")
            access_token = result['access_token']
            headers = {'Authorization': f'Bearer {access_token}'}

        return headers

    def _initialize_client(self):
        
        if self.client_type == 'onedrive':
            # from msgraph import GraphServiceClient
            # from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions

            import msal
            import requests

            # get the MS EntraID App Client_id - for now assume in a .env file or defined as env var
            import importlib.util
            dotenv_spec = importlib.util.find_spec("dotenv")
            if dotenv_spec is not None:
                from dotenv import load_dotenv
                load_dotenv()

            CLIENT_ID = os.getenv('CLIENT_ID')
            AUTHORITY = f'https://login.microsoftonline.com/consumers'


            # Define your application (client) ID and the scopes required
            client_id = CLIENT_ID
            scopes = ['Files.ReadWrite', 'User.Read']  # Add other scopes as needed
            self.scopes = scopes
            self.username = None # initialize to None and update on first login - only used to maintain in-memory token cache

            # Create a public client application
            self.token_cache = msal.TokenCache() # The TokenCache() is in-memory.
            self.app = msal.PublicClientApplication(CLIENT_ID, 
                                               authority=AUTHORITY,
                                               token_cache=self.token_cache
                                               )

            initial_header = self._acquire_token() # call to get an initial token up-front
            
            #set the drive id and item id to be used in api calls:
            self.drive_id = self.excel_path.split('!')[0]
            self.item_id = self.excel_path

            #ensure we use the user's preferred ssl context
            import ssl
            if self.custom_cert_store == 'certifi':
                import certifi
                self.ssl_context = ssl.create_default_context(cafile=certifi.where())
            else:
                self.ssl_context = ssl.create_default_context()

        else:
            raise NotImplementedError("Only OneDrive client type is currently supported")
        

    def _initialize_ducklake(self):
        """ ATTACH to the ducklake. Ensure the needed sheets exist in the Excel file. 
        If they exist already, do an initial _pull. If not, do a _push """

        self.db.sql(f""" ATTACH 'ducklake:{self.local_catalog}' AS {self.ducklake_name} (DATA_PATH '{self.data_path}') """)

        tables = self.db.sql(f""" SELECT table_name FROM information_schema.tables where table_catalog like '__ducklake_metadata_{self.ducklake_name}' """).fetchall()

        self.catalog_tables = [t[0] for t in tables] # keep a list of table names, to iterate over later
        self.catalog_tables_no_ducklake = [t.replace('ducklake_','',1) for t in self.catalog_tables] # Excel sheetnames limited to 31 characters. Use shortened version

        if self.client_type == 'onedrive':
            # list sheets in excel file:
            remote_sheetnames, session_id = self.loop.run_until_complete(self._get_existing_sheets())

            # loop over catalog_tables and create ones that don't exist already
            all_sheets_exist_already = True # flag whether or not we start with a pull (if True) or a push (if False)
            missing_tables = []
            for t in self.catalog_tables_no_ducklake:
                if t in remote_sheetnames:
                    continue
                else:
                    all_sheets_exist_already = False
                    missing_tables.append(t)
            
            if len(missing_tables) > 0:
                # Create missing sheets concurrently
                self.loop.run_until_complete(self._create_sheets(missing_tables,session_id))

            if all_sheets_exist_already:
                # initialize state from the remote catalog
                # for now, this will error if the sheets exist but no column headers in sheet
                self._pull()
            else:
                # reset the remote to match the state of the local
                self._push()

            # close the workbook session
            self.loop.run_until_complete(self._close_workbook_session(session_id))

        else:
            raise NotImplementedError("Only OneDrive client type is currently supported")


    async def _create_workbook_session(self, persist_changes: bool) -> str:
        """Create a session id to tag a set of concurrent API calls with"""
        headers = self._acquire_token()
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/createSession"
        body = {"persistChanges": persist_changes}
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context, force_close=True)) as session:
            data = await self._request_with_retry(session, 'post', url, headers=headers, json=body)
        return data.get('id')


    async def _close_workbook_session(self, session_id: str) -> None:
        """Close the workbook session"""
        headers = self._acquire_token()
        headers.update({'workbook-session-id': session_id})
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/closeSession"
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context, force_close=True)) as session:
            await self._request_with_retry(session, 'post', url, headers=headers)


    async def _request_with_retry(self, session: aiohttp.ClientSession, method: str, url: str, **kwargs) -> dict:
        """Wrapper to handle 429 and respect Retry-After header"""
        retry_for_404_limit = 5 # sometimes queries for ranges from the enpoint 404 it has't caught up to the sheet creation
        retries_for_404 = 0
        while True:
            async with session.request(method, url, **kwargs) as resp:
                if (resp.status == 404) and (retries_for_404 < retry_for_404_limit):
                    retries_for_404 += 1
                    print(f'{retries_for_404 = }')
                    await asyncio.sleep(1.0)
                elif resp.status != 429:
                    resp.raise_for_status()
                    return await resp.json(content_type=None) # sometimes we have no response body - ignore errors that would throw by setting content_type=None
                else:
                    retry_after = int(resp.headers.get('Retry-After', '1'))
                    await asyncio.sleep(retry_after)


    async def _get_existing_sheets(self) -> tuple:
        """Create a workbook session for initialization and fetch existing sheet names"""
        session_id = await self._create_workbook_session(persist_changes=True) # this call won't make changes, but if we have to create sheets, subsequent ones will reuse session_id
        headers = self._acquire_token()
        headers.update({'Workbook-Session-Id': session_id})

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context, force_close=True)) as session:
            response = await self._request_with_retry(
                session, 'get',
                f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets",
                headers=headers
            )
            sheet_names = [s['name'] for s in response.get('value', [])]

        return sheet_names, session_id
  

    async def _create_sheets(self, tables_to_create, session_id):
        headers = self._acquire_token()
        headers.update({'Workbook-Session-Id': session_id})
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context, force_close=True)) as session:
            tasks = []
            for t in tables_to_create:
                add_url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets/add"
                body = {"name": t}
                tasks.append(self._request_with_retry(session, 'post', add_url, headers=headers, json=body))
            await asyncio.gather(*tasks)

            
    def sql(self,query):
        """Run a DuckDB query. 
        Before the query, update the local catalog from the Excel. 
        After the query overwrite the remote Excel catalog
        Then return the original result"""

        self._pull()
        result = self.db.sql(query)
        self._push()

        return result
    

    def _pull(self):
        """ Iterate over all the catalog tables
        For each table, get the current values in the remote
        Accumulate the updates in a dict of table_name/dataframe, 
        and then truncate/overwrite all the local metadata tables
        Because everything coming from Excel may be a string, we also need to
        select 0 rows from the target table to grab the schema, so we can coerce the pandas df accordingly
        """
            
        # Determine dtypes for each table based on existing ducklake table schemas
        table_dtype_map = {}
        for t in self.catalog_tables_no_ducklake:
            t_dtypes = self.db.sql(f""" SELECT * FROM __ducklake_metadata_{self.ducklake_name}.{'ducklake_'+t} WHERE 1=0 """).df().dtypes
            # replace any int dtype with Int64 for nullable ints from pandas
            t_dtypes = t_dtypes.replace({
                'int32': 'Int64',
                'int64': 'Int64'
            })
            table_dtype_map[t] = t_dtypes

        if self.client_type == 'onedrive':
            # Run async get for all tables
            metadata_to_write = self.loop.run_until_complete(self._session_pull_all(table_dtype_map))
        else:
            raise NotImplementedError("Only OneDrive client type is currently supported")

        # write each table to the ducklake tables in DuckDB
        for t, df in metadata_to_write.items():
            this_table = f"__ducklake_metadata_{self.ducklake_name}.{'ducklake_'+t}"
            self.db.sql(f"""BEGIN TRANSACTION;
                            TRUNCATE {this_table};
                            INSERT INTO {this_table} SELECT * FROM df; 
                            COMMIT;""")


    async def _session_pull_all(self, table_dtype_map):
        """Create async task queue of all get requests for list of tables"""
        session_id = await self._create_workbook_session(persist_changes=False)
        headers = self._acquire_token()
        headers.update({'workbook-session-id': session_id})

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context, force_close=True)) as session:
            tasks = []
            for t, t_dtypes in table_dtype_map.items():
                tasks.append(self._async_pull_table(session, t, t_dtypes, headers)
            )
            results = await asyncio.gather(*tasks)

        await self._close_workbook_session(session_id)
        return dict(results)
    

    async def _async_pull_table(self, session, t, t_dtypes, headers) -> tuple:
        """ Get "used range" of sheet, which includes the values in the response.
         Then convert to a pandas dataframe """
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/usedRange"
        used_range = await self._request_with_retry(session, 'get', url, headers=headers)
        values = used_range.get('values', []) # will be a list of lists - each internal list is a row
        # convert to dict, then map to dataframe with correct types
        if len(values) > 1: # will be 1 with just header row
            keys = values[0]
            rows = values[1:]
            # Transpose to columns
            cols = list(zip(*rows))
            new_data_dict = {
                col: [None if v == '' else v for v in col_vals] # replace empty string '' with Python None, in case it goes in numeric column
                for col, col_vals in zip(keys, cols)
            }
            df = pd.DataFrame({col: pd.Series(new_data_dict[col], dtype=dt) for col, dt in t_dtypes.items()})
            # the ducklake_metadata table stores the encryption value as a string of 'true' or 'false
            # the roundtrip to Excel turns it into an Excel Boolean. since it's now a string again, need to 
            # convert to the expected case
            if t == 'metadata':
                df.loc[df.key == 'encrypted', 'value'] = df.loc[df.key == 'encrypted', 'value'].astype(str).str.lower()
        else:
            # create empty table with correct schema
            df = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in t_dtypes.items()})
        return (t, df)


    def _push(self):
        """ Iterate over all the catalog tables (concurrently with aiohttp)
        For each table, clear the current values in the remote (get used range and clear cells), 
        then replace with the full contents of the local
        """
        table_df_map = {}
        for t in self.catalog_tables_no_ducklake:
            t_df = self.db.sql(f""" SELECT * FROM __ducklake_metadata_{self.ducklake_name}.{'ducklake_'+t} """).df()
            for col in t_df.columns:
                # remove timezone localization, if any
                if pd.api.types.is_datetime64_any_dtype(t_df[col]):
                    if getattr(t_df[col].dt, 'tz', None) is not None:
                        t_df[col] = t_df[col].dt.tz_convert('UTC').dt.tz_localize(None)
            table_df_map[t] = t_df

        if self.client_type == 'onedrive':
            # Run async push for all tables
            self.loop.run_until_complete(self._session_push_all(table_df_map))
        else:
            raise NotImplementedError("Only OneDrive client type is currently supported")


    async def _session_push_all(self, table_df_map):
        session_id = await self._create_workbook_session(persist_changes=True)
        headers = self._acquire_token()
        headers.update({'workbook-session-id': session_id})

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context, force_close=True)) as session:
            tasks = []
            for t, t_df in table_df_map.items():
                tasks.append(self._async_push_table(session, t, t_df, headers))
            await asyncio.gather(*tasks)

        await self._close_workbook_session(session_id)


    async def _async_push_table(self, session, t, t_df,headers):
        # Fetch used range to clear
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/usedRange"
        used_range = await self._request_with_retry(session, 'get', url, headers=headers)
        
        # clear used range of sheet
        clear_range = used_range['address'].split('!')[1] #splitting to get the range and not the sheetname
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/range(address='{clear_range}')/clear"
        body = {
            "apply_to": 'All'
        }
        clear_response = await self._request_with_retry(session,'post', url, headers=headers, json=body)

        # prep table values for patch request to update values in (cleared) range
        replace_map = {'<NA>': None, 'nan': None, 'None': None, 'NaT': None} # deal with the variety of ways a pandas NULL can be serialized to string
        values = [t_df.columns.tolist()] + t_df.astype(str).replace(replace_map).values.tolist()

        range_address = f"A1:{string.ascii_uppercase[t_df.shape[1]-1]}{t_df.shape[0]+1}"

        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/range(address='{range_address}')"

        # Request body with the values
        body = {
            "values": values
        }

        # Make the PATCH request to write data
        headers_patch = headers.copy()
        headers_patch['Content-Type'] = 'application/json' # add content type
        patch_resp = await self._request_with_retry(session, 'patch', url, headers=headers_patch, json=body)
        #print(f"Pushed {t}")         


if __name__ == '__main__':
    import time
    from dotenv import load_dotenv
    load_dotenv()
    MY_TEST_ONEDRIVE_PATH = os.getenv('MY_TEST_ONEDRIVE_PATH')
    print('creating test instance:')
    start_time = time.time()
    test = DuckLakeXL(
        excel_path=MY_TEST_ONEDRIVE_PATH,
        data_path='../test/',
        ducklake_name='my_excel_ducklake',
        #encrypted=False, 
    )
    print(f'Initialization took {time.time() - start_time:.2f} seconds')

    time.sleep(1.0)

    print('initialized...')
    start_time = time.time()
    test.sql("""USE my_excel_ducklake;
            CREATE TABLE my_table(id INTEGER, val VARCHAR);""")
    print(f'Table creation took {time.time() - start_time:.2f} seconds')

    time.sleep(1.0)

    print('table created...')
    start_time = time.time()
    test.sql("""INSERT INTO my_table VALUES
               (1, 'alpha'),
               (2, 'beta'),
                (3, 'gamma'),
                (4, 'delta');               
               """)
    print(f'Data insertion took {time.time() - start_time:.2f} seconds')

    time.sleep(1.0)

    print('data inserted...')
    start_time = time.time()
    test.sql("""SELECT * FROM my_table;""").show()
    print(f'First select took {time.time() - start_time:.2f} seconds')

    time.sleep(1.0)

    start_time = time.time()
    test.sql("""DELETE FROM my_table WHERE id = 3;""")
    print(f'Delete operation took {time.time() - start_time:.2f} seconds')

    time.sleep(1.0)

    start_time = time.time()
    test.sql("""SELECT * FROM my_table;""").show()
    print(f'Final select took {time.time() - start_time:.2f} seconds')