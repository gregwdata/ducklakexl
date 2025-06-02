import duckdb
import pandas as pd
import os
import asyncio
import string
import requests

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
        self._pick_client()
        self._initialize_client()

        # the MSGraph sdk is all async coroutines - for initial simplicity, keep this class all sync
        # whenever we call one of the graph functions, need to wrap in a loop:
        self.loop = asyncio.new_event_loop()

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
        else:
            raise NotImplementedError("Only OneDrive client type is currently supported")
        

    def _initialize_ducklake(self):
        """ ATTACH to the ducklake. Ensure the needed sheets exist in the Excel file. 
        If they exist already, do an initial _pull. If not, do a _push """

        self.db.sql(f""" ATTACH 'ducklake:{self.local_catalog}' AS {self.ducklake_name} (DATA_PATH '{self.data_path}') """)

        tables = self.db.sql(f""" SELECT table_name FROM information_schema.tables where table_catalog like '__ducklake_metadata_{self.ducklake_name}' """).fetchall()

        self.catalog_tables = [t[0] for t in tables] # keep a list of table names, to iterate over later
        self.catalog_tables_no_ducklake = [t.replace('ducklake_','',1) for t in self.catalog_tables] # Excel sheetnames limited to 31 characters. Use shortened version

        # list sheets in excel file:
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets"
        headers = self._acquire_token()
        worksheets = requests.get(url, headers=headers)
        worksheets.raise_for_status()
        remote_sheetnames = [s['name'] for s in worksheets.json()['value']]
        print(remote_sheetnames)

        # loop over catalog_tables and create ones that don't exist already
        all_sheets_exist_already = True # flag whether or not we start with a pull (if True) or a push (if False)
        for t in self.catalog_tables_no_ducklake:
            if t in remote_sheetnames:
                continue
            else:
                all_sheets_exist_already = False

                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets/add"
                body = {
                    "name": t
                }
                headers = self._acquire_token()
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                print(f'Added: {t}')


        if all_sheets_exist_already:
            # initialize state from the remote catalog
            # for now, this will error if the sheets exist but no column headers in sheet
            self._pull()
        else:
            # reset the remote to match the state of the local
            self._push()
        
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
        metadata_to_write = {}
        for t in self.catalog_tables_no_ducklake:
            t_dtypes = self.db.sql(f""" SELECT * FROM __ducklake_metadata_{self.ducklake_name}.{'ducklake_'+t} WHERE 1=0 """).df().dtypes
            # replace any int dtype with Int64 for nullable ints from pandas
            t_dtypes = t_dtypes.replace({
                'int32': 'Int64',
                'int64': 'Int64'
            })

            if self.client_type == 'onedrive':
                # get used range of sheet
                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/usedRange"
                headers = self._acquire_token()
                used_range = requests.get(url, headers=headers)
                used_range.raise_for_status()
                print(t)
                t_new_data = used_range.json()['values'] # will be a list of lists - each internal list is a row
                # convert to dict, then map to dataframe with correct types
                if len(t_new_data) > 1: # will be 1 with just header row
                    keys = t_new_data[0]
                    values = list(zip(*t_new_data[1:]))
                    new_data_dict = dict(zip(keys, values))
                    print(t_new_data)
                    print(values)
                    print(new_data_dict,flush=True)
                    print('printed new data_dict',flush=True)
                    # replace empty string '' with Python None, in case it goes in numeric column
                    new_data_dict = {k: [None if v == '' else v for v in vals] for k, vals in new_data_dict.items()}                       
                    print(new_data_dict)
                    metadata_to_write[t] = pd.DataFrame({col: pd.Series(new_data_dict[col],dtype=dt) for col, dt in t_dtypes.items()})
                    # the ducklake_metadata table stores the encryption value as a string of 'true' or 'false
                    # the roundtrip to Excel turns it into an Excel Boolean. since it's now a string again, need to 
                    # convert to the expected case
                    if t == 'metadata':
                        print(metadata_to_write[t])
                        print(metadata_to_write[t].dtypes)
                        metadata_to_write[t].loc[metadata_to_write[t].key=='encrypted','value'] = metadata_to_write[t].loc[metadata_to_write[t].key=='encrypted','value'].astype(str).str.lower()
                else:
                    # create empty table
                    metadata_to_write[t] = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in t_dtypes.items()})

            else:
                raise NotImplementedError("Only OneDrive client type is currently supported")
            
        for t in self.catalog_tables_no_ducklake:
            this_table = f"""__ducklake_metadata_{self.ducklake_name}.{'ducklake_'+t}"""
            this_df = metadata_to_write[t]
            self.db.sql(f"""BEGIN TRANSACTION;
                            TRUNCATE {this_table};
                            INSERT INTO {this_table} SELECT * FROM this_df; 
                            COMMIT;""")



    def _push(self):
        """ Iterate over all the catalog tables
        For each table, clear the current values in the remote (get used range and clear cells), 
        then replace with the full contents of the local
        """
        for t in self.catalog_tables_no_ducklake:
            t_df = self.db.sql(f""" SELECT * FROM __ducklake_metadata_{self.ducklake_name}.{'ducklake_'+t} """).df()


            if self.client_type == 'onedrive':
                # get used range of sheet
                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/usedRange"
                headers = self._acquire_token()
                used_range = requests.get(url, headers=headers)
                used_range.raise_for_status()
                print(t)
                #print(result)
                # clear used range of sheet
                clear_range = used_range.json()['address'].split('!')[1] #splitting to get the range and not the sheetname
                # if clear_range == 'A1':
                #     clear_range = 'A1:A1'
                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/range(address='{clear_range}')/clear"
                body = {
                    "apply_to": 'All'
                }
                clear_response = requests.post(url, headers=headers, json=body)
                clear_response.raise_for_status()

                # Patch method on range not implemented in msgraph python sdk yet - fall back to requests:
                for col in t_df.columns:
                    # remove timezone localization, if any
                    if pd.api.types.is_datetime64_any_dtype(t_df[col]):
                        if getattr(t_df[col].dt, 'tz', None) is not None:
                            t_df[col] = t_df[col].dt.tz_convert('UTC').dt.tz_localize(None)
                replace_map = {'<NA>': None, 'nan': None, 'None': None, 'NaT': None} # deal with the variety of ways a pandas NULL can be serialized to string
                values = [t_df.columns.tolist()] + t_df.astype(str).replace(replace_map).values.tolist()
                print(values)

                range_address = f"A1:{string.ascii_uppercase[t_df.shape[1]-1]}{t_df.shape[0]+1}"

                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{self.item_id}/workbook/worksheets('{t}')/range(address='{range_address}')"

                # Request body with the values
                body = {
                    "values": values
                }

                # Make the PATCH request to write data
                headers['Content-Type'] = 'application/json' # add content type
                response = requests.patch(url, headers=headers, json=body)
                print(response.text)
                print(response.reason)
                response.raise_for_status()
            else:
                raise NotImplementedError("Only OneDrive client type is currently supported")

            

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    MY_TEST_ONEDRIVE_PATH = os.getenv('MY_TEST_ONEDRIVE_PATH')
    
    print('creating test instance:')
    test = DuckLakeXL(
        excel_path=MY_TEST_ONEDRIVE_PATH,
        data_path='./test/',
        ducklake_name='my_excel_ducklake',
        #encrypted=False, 
    )

    print('initialized...')
    test.sql("""USE my_excel_ducklake;
            CREATE TABLE my_table(id INTEGER, val VARCHAR);""")
    print('table created...')
    test.sql("""INSERT INTO my_table VALUES
               (1, 'alpha'),
               (2, 'beta'),
                (3, 'gamma'),
                (4, 'delta');               
               """)
    print('data inserted...')

    test.sql("""SELECT * FROM my_table;""").show()

    test.sql("""DELETE FROM my_table WHERE id = 3;""")

    test.sql("""SELECT * FROM my_table;""").show()