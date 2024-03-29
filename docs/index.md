# Getting Started
A library to help create python pipelines for Vayner X companies by providing easy to use functions for performing common data pipeline tasks

This is primarily for working with Social Media Organic and Paid Advertisement Data but also contains
generic functions for writing to postgreSQL and snowflake databases, creating notifications in Slack Channels and writing to Google Sheets

## Library Installation
```
pip install veetility
```
To upgrade the library after an update
```
pip install veetility --upgrade
```
## Initialisation of the UtilityFunctions class
An instance of the "UtilityFunctions" class can be initiated with the ability to read/write with 
google sheets and also to read/write to tables in a postgreSQL database. However none of these
are mandatory.

### Google Sheets Authentication
In order to use the functions that read/write from google sheets then a dictionary
needs to be passed to the "gspread_auth_dict" argument. 
This dictionary can be obtained from a google API credentials JSON file.

The instructions for creating this JSON file can be found from the [gspread documentation](https://docs.gspread.org/en/latest/oauth2.html)

Then copy the JSON information in curly brackets as a python dictionary into the gspread.service_account_from_dict() authentication method. The "client_email" parameter in the JSON material is the email address of the google sheet client we have created. **This email address must be given read or write access to any google sheet you want to 
interact with.**

### PostgreSQL connection parameters
The following correction parameters need to be passed in during initialisation of the class instance

1. db_name e.g. "vayner_external_XXXXXXXXXXX"
2. db_host e.g. "dashboard-vayner-XXXXXXXXXXXXXXXXXXXX.rds.amazonaws.com"
3. db_port e.g. "5432" (this is the default for postgreSQL)
4. db_user e.g. "tom.jones"
5. db_password e.g. "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

## Setup in code
It is advisable to put the parameters into a config.py file (cf) which can then be imported into
the main script

```
import config as cf
from veetility import utility_functions

util = utility_functions.UtilityFunctions(cf.google_sheet_auth_dict,cf.db_user, 
                                          cf.db_password,cf.db_host,
                                          cf.db_port, cf.db_name)
```

## Usage in code
Once an instance of the class UtilityFunctions has been created, e.g. "util" then functions (methods) 
of the class can be used e.g. "write_to_gsheet()".

This can write a pandas.DataFrame to any google sheet the "client_email" has been given "editor" access to.
First pass in the name of the Google Sheet workbook, then the tab of the workbook, then the pandas.DataFrame
```
util.write_to_gsheet("Indeed Data Error Tracking","PaidNoBoostedMatch", 
                        paid_no_boosted_match)
```
