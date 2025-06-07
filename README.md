# ![](https://github.com/gregwdata/ducklakexl/raw/main/readme_images/ducklake_logo_white_small.png) DuckLakeXL

> [!CAUTION]
> This package is based on taking a stupid idea way too far. It is not suited for production use, but may have some paedogogical utility. Or you may just find it fun to mess around with. It is slow, non-ACID, and all around silly. 

This package allows the use of Excel (as a local file or on OneDrive/SharePoint) as a catalog database for [DuckLake](https://ducklake.select/), using the [ducklake extension](https://duckdb.org/docs/stable/core_extensions/ducklake) in DuckDB.

![Screen recording showing DuckLakeXL query based on the catalog shown in both OneDrive web UI and via the Excel App, along with the parquet files landing in the file store.](https://github.com/gregwdata/ducklakexl/raw/main/readme_images/ducklakeXL_insert_into_table_3view.gif)

## Why???

DuckLake (re-)[^1]implements a catalog for a lakehouse architecture by leveraging commonly-used database management systems (DBMS) like PostreSQL or SQLite. 

However, the initial implementation of the extension left out the **GOAT** of DMBSs: Excel workbooks. Anyone who has worked for more than 5 minutes in an enterprise more than 30 miles outside San Fransisco know that the vast majority of information in the enterprise is cataloged and transacted via Excel spreadsheets. And if you're lucky, these spreadsheets are accessible to more than one person at a time via platforms like SharePoint.

The pre-modern data stack is built on a foundation of Excel workbooks, SharePoint folders, and network drives. It is only fitting that users constrained to this stack get the benefit of modern data lake concepts and open table formats.  

[^1]: Many have noted that it's perhaps a recapitulation of the Hive metastore concept

## How?

Though it would have been nice to directly attach to an Excel file, the official [Excel Extension](https://duckdb.org/docs/stable/core_extensions/excel.html) doesn't even do that, so what hope did this silly project have?

The architecture used is a sync between a local DuckDB-backed ducklake instance and a "remote" Excel copy of the ducklake metadata tables. Every `sql` operation against the `DuckLakeXL` instance passes the query through to an underlying DuckDB connection, to which the DuckLake has been attached. The calls to the DuckDB connection are wrapped with syncing operations to (1) ensure that the local copy of the metadata is updated with the latest version of the remote before executing the query and (2) propagate any changes to the Excel remote after the operation completes. 

Is this a robust and reliable approach? No way! Is it a practical way to persist a DuckLake metadata in Excel with minimal effort on the implementation side? Yeah. 

In a world of single-user, using a local Excel file, where we got lucky and nothing ever failed mid-transaction, this would give you a reasonably reliable method. When we get into multiple HTTP requests against OneDrive or Sharepoint that need to all succeed to maintain valid state... things start to get iffy. When we then consider multiple users potentially running concurrent operations against a OneDrive/Sharepoint remote, that opens up all the concurrency and conflict management problems that DuckLake solves by usnig a real DBMS with transactional behavior, like PostgreSQL, that are designed around those kind of problems. While it would be a waste of time and effort to fully solve them for this project, there are probably ways to nibble around the edges and make it incrementally more capable of (but let's remember, not well-suited for!) this kind of usage over time. 

## Usage

### Installation

Install from PyPI using

    pip install ducklakexl

or

    uv add ducklakexl

Alternatively, clone this repo and customize!

### Local Excel Files

If the input to `excel_path` is a string ending in `.xlsx` and `drive_id` is not specified, DuckLakeXL will treat that as a local excel file (which could also be on a network share that you have read/write permission on).

If the specified file does not exist, DuckLakeXL will attempt to create it.

If it already exists, DuckLakeXL will check for existing sheets that correspond to the names of DuckLake metadata tables. If all are present, the DuckLake is initialized with the values from those tables. (Note that it does not check validity of the data in the sheets, or presents of headers - only sheetnames. Errors will result if no headers present, or data on the sheets are otherwise misaligned to the schema of the ducklake metadata tables.)

Since Excel maintains a lock on a `.xlsx` file when it is open, the file must be closed for DuckLakeXL to use it. You can open the file after a query to see the query's effects on the metadata.

```python
# if running from a Jupyter session, you may need to run these two lines invoking nest_asyncio
# so the async event loop in DuckLakeXL won't conflict with the Jupyter kernel's event loop
import nest_asyncio # needed when calling from Jupyter
nest_asyncio.apply()

from ducklakexl import DuckLakeXL

# Create a DuckLakeXL instance
db = DuckLakeXL(
    excel_path='/path/to/local/or/network/file.xlsx',
    data_path='/path/to/local/or/network/directory/',
    ducklake_name='my_excel_ducklake',
)

# Execute SQL calls thusly
db.sql("""USE my_excel_ducklake;
        CREATE TABLE my_table(id INTEGER, val VARCHAR);
        INSERT INTO my_table VALUES
        (1, 'Excel Rules!');
        """) 

# the sql method just returns the DuckDB result of the sql method, 
# so you can invoke any of its methods:
db.sql("""SELECT * FROM my_table""").show()
my_df = db.sql("""SELECT * FROM my_table""").df()
```

### OneDrive

Other than the initialization of the `DuckLakeXL` object shown below, usage on OneDrive is the same as local Excel files. Refer to the example usage and imports above.

One bonus with using Excel files on OneDrive: you can keep the workbook open, in browser or in the local Excel app, and DuckLakeXL can still read from and write to it!

If you already have an excel file in OneDrive that you want to use, there are two ways to reference it:
- using the OneDrive `item id` you can find by opening the file in OneDrive and looking at the URL's `resid` query parameter, as in:
`https://onedrive.live.com/personal/a123456789abcdef/_layouts/15/Doc.aspx?resid=`**`A123456789ABCDEF!s0123456789abcdef0123456789abcdef`**`&cid=a123456789abcdef&migratedtospo=true&app=Excel` 
    ```python
    # Create a DuckLakeXL instance using the "resid"
    db = DuckLakeXL(
        excel_path='A123456789ABCDEF!s0123456789abcdef0123456789abcdef',
        data_path='/path/to/local/or/network/directory/',
        ducklake_name='my_excel_ducklake',
    )
    ```

- Setting the `drive_id` parameter to the drive id obtained from the OneDrive URL (`A123456789ABCDEF` in the above, not case sensitive) and setting the `excel_path` parameter to the name of the Excel file, as in the below. Optionally, you can specify a `folder_path` if the file is not at the root of the specified drive.
    ```python
    # Create a DuckLakeXL instance using a OneDrive file specified by name
    db = DuckLakeXL(
        excel_path='my_onedrive_excel_file.xlsx',
        data_path='/path/to/local/or/network/directory/',
        ducklake_name='my_excel_ducklake',
        drive_id='A123456789ABCDEF',
        folder_path='foldername/subfolder'
    )
    ```

The above will throw an exception if the file does not exist. If you want to create a new OneDrive Excel file as a DuckLakeXL metadata store on initialization, you can specifiy the name of a file that does not exist, and set `create_if_missing = True`. The file will be created.

```python
# Create a DuckLakeXL instance by creating a new OneDrive file, specified by name
db = DuckLakeXL(
    excel_path='my_onedrive_excel_fil_that_does_not_exist_yet.xlsx',
    data_path='/path/to/local/or/network/directory/',
    ducklake_name='my_excel_ducklake',
    drive_id='A123456789ABCDEF',
    folder_path='foldername/subfolder',
    create_if_missing=True
)
```

### OneDrive/SharePoint Setup

You need to register an app with Entra ID in your (organization's) Azure Portal. That enables programatic API calls against the Microsoft Graph endpoints, with delegated permissions, and ability to use `Files.ReadWrite` and `User.Read` scopes.

Here are approximate steps to follow to set up API access for a personal OneDrive:

1. Go to [portal.azure.com](portal.azure.com). You'll need to set up an account or do an initial sign in to portal with your microsoft account.
2. Search in the search bar for `Entra ID` and go to that page
3. In the menu on the left, select `Manage > App registrations`
4. Click `+` to create a new registration. 
    1. Give it a meaningful name. 
    2. For `Supported account types`, the current configuration of DuckLakeXL is set up and tested based on selecting `Personal Microsoft accounts only`. Adding and testing the ability to authenticate via an organizational tenant may come at some point in the future.
    3. For redict URI, select `Public Client / Native`, and enter `http://localhost` for redirect URI
5. On the `Authentication` page, ensure that `Access tokens` is selected under the heading Implicit grant and hybrid flows heading.
6. On the `API Permissions` page, add the following Microsoft Graph permissions: `Files.ReadWrite`, `User.Read`, `profile`, `offline_access`, and optionally `Files.ReadWrite.All`. This last one will allow DuckLakeXL to use files that others own and have shared with you. Its use is toggled with the DuckLakeXL initialization parameter `read_shared_files`.
7. Record the Application (client) ID from the `Overview` page. DuckLakeXL expects this value to either be stored in an environment variable called `CLIENT_ID` or saved as such in a `.env` file in the path of your Python script.

## TODOs

No commitment is made that any of these actually get done. However, PRs are welcome.

- [x] Local `xlsx` file
- [x] OneDrive
- [x] OneDrive API calls async-ify
- [ ] Make it ACID (one-drive)?
    - Lazy way: 
        - A semafore strategy on a separate sheet
    - Better way: 
        - Abstract out an append-only SCD-style version of each ducklake table. 
        - Convert into/out of that to current version of each table as needed.
        - Robustness against case where new lines appended concurrently?
- [ ] Ensure functionality for SharePoint matches that on OneDrive 
- [ ] Logging - optionally pass in a user-provided logger as parameter
- [ ] Logging - use logger
- [ ] On push, only write changed tables
    - Cache ducklake tables on pull, compare before push to identify changes
    - More CDC/wal way to do it? Maybe take advantage of `ducklake_name.snapshots()` function
    - Just append instead of clear and write, where applicable
- [ ] Meta-time-travel by leveraging file versions stored on OneDrive/SharePoint?
    - This could be a way to roll back on ACID-related issues above, if we fail mid-transaction
- [ ] Validate use of a network share (mounted or not) as a `data_path`
- [ ] Is it possible to use SharePoint as the data path?
    - Possibly if we have OneDrive syncing
    - Probably not via Graph API
- [ ] Tests
- [ ] CI/CD
- [ ] Publish to pypi?