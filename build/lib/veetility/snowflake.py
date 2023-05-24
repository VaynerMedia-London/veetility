#%%
#import env
import pandas as pd
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL
from snowflake.snowpark import Session
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer
from datetime import datetime
import time
from sqlalchemy import create_engine
#import utility_functions as uf
#%%


#%%
class Snowflake():
    
    def __init__(self,connection_params_dict,client_name) -> None:
        """Initialise the snowflake connection with a dictionary of connection parameters.

        Log File is created in the LOG_DIR environment variable with a subdirectory with the name of the client_name
        
        Args:
            connection_params_dict (dict): Dictionary of connection parameters
            client_name (str): Name of the client, this is used to create a folder in the LOG_DIR environment variable
        
        Returns:
            None"""
        
        self.connection_params_dict = connection_params_dict

        self.user = self.connection_params_dict['user']
        self.role = self.connection_params_dict['role']
        self.warehouse = self.connection_params_dict['warehouse']
        self.database = self.connection_params_dict['database']
        self.schema = self.connection_params_dict['schema']
        self.account = self.connection_params_dict['account']
        self.password = self.connection_params_dict['password']
        #self.session = Session().create(self.connection_params_dict)
        self.session = Session.builder.configs(self.connection_params_dict).create()
        #self.logger = Logger(client_name,'snowflake')
    
    def test_snowflake_connection(self):
        """Function to test the snowflake connection"""
        print(self.session.sql('''SELECT 'Connected!' as STATUS''').collect())
        # Show databases
        df_test = self.session.sql('SHOW DATABASES')
        df_test.show()
        # Show roles
        #self.logger.info(self.session.sql(f'''USE ROLE {self.role}''').collect())
        print(self.session.sql(f'''USE ROLE {self.role}''').collect())

    def get_data_with_URL(self, data_table, schema=None, chunk_size=200000):
        """Function to read Snowflake table using SQLAlchemy
        
        URL stands for Uniform Resource Locator. It is a reference (an address) to a resource on the Internet.
        
        Args:
            data_table (str): Name of the table to read
            schema (str, optional): Name of the schema to read from. Defaults to the schema specified on class initialisation.
            chunk_size (int, optional): Size of the data chunks to read in a single batch. Defaults to 200000.
        
        Returns:
            df: Pandas dataframe of the data read from Snowflake"""

        if schema == None:
            schema = self.schema

        url = URL(user=self.user,password=self.password,account=self.account,
                    warehouse=self.warehouse,database=self.database,schema=schema,role = self.role)
        
        engine = create_engine(url)
        connection = engine.connect()

        # count the total number of rows
        count_query = f"SELECT COUNT(*) FROM {schema}.{data_table}"
        #count_query = f'''select count(*) from "{data_table}"'''
        total_rows = pd.read_sql(count_query, connection).values[0][0]

        print(f"Total rows in {data_table}: {total_rows}")
        if total_rows == 0:
            print("No data in the table.")
            return pd.DataFrame()

        df = pd.DataFrame()

        # read the data in chunks
        query_data = f"SELECT * FROM {schema}.{data_table}"
        #query_data = f'''select * from "{data_table}" '''
        for chunk in pd.read_sql(query_data, connection, chunksize=chunk_size):
            df = pd.concat([df, chunk])

        df.info
        return df
  

    def create_table_from_dataframe(self,df,table_name,database=None, schema=None):
        '''Define function to create Snowflake table from Pandas dataframe
        
        Args:
            df (dataframe): Pandas dataframe to create table from
            table_name (str): Name of the table to create
            database (str, optional): Name of the database to create the table in. Defaults to the database specified on class initialisation.
            schema (str, optional): Name of the schema to create the table in. Defaults to the schema specified on class initialisation.
        
        Returns:
            None'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        # Create table
        self.session.createDataFrame(df).createOrReplaceTempView(table_name)
        # Create table in Snowflake
        self.session.sql(f'''CREATE OR REPLACE TABLE "{database}"."{schema}"."{table_name}" AS SELECT * FROM {table_name}''').collect()
    
    def drop_table(self,table_name,database=None, schema=None):
        '''Function to drop Snowflake table
        
        Args:
            table_name (str): Name of the table to drop
            database (str, optional): Name of the database to drop the table from. Defaults to the database specified on class initialisation.
            schema (str, optional): Name of the schema to drop the table from. Defaults to the schema specified on class initialisation.
        
        Returns:
            None'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        # Drop table
        self.session.sql(f'''DROP TABLE "{database}"."{schema}"."{table_name}"''').collect()
    
    def write_df_to_snowflake(self, df, table_name, database=None, schema=None,
                          auto_create_table=False, overwrite=False, chunk_size=200000):
        '''Function to write Pandas dataframe to Snowflake table

        Truncates (if it exists) or creates new table and inserts the new data into the selcted table
        
        Args:
            df (dataframe): Pandas dataframe to write to Snowflake
            table_name (str): Name of the table to write to
            database (str, optional): Name of the database to write the table to. Defaults to the database specified on class initialisation.
            schema (str, optional): Name of the schema to write the table to. Defaults to the schema specified on class initialisation.
            auto_create_table (bool, optional): If True, creates the table if it does not exist. Defaults to False.
            overwrite (bool, optional): If True, overwrites the table if it exists. Defaults to False.
            chunk_size (int, optional): Size of the data chunks to write in a single batch. Defaults to 200000.
        
        Returns:
            None'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database

        rows = df.shape[0]
        batches = [df[i:i + chunk_size] for i in range(0, rows, chunk_size)]
        
        for i, batch in enumerate(batches):
            overwrite_batch = overwrite if i == 0 else False  # overwrite for first batch, append for others
            now = time.time()
            
            try:
                self.session.write_pandas(batch, table_name, parallel=8,schema=schema,database=database,
                                        auto_create_table=auto_create_table,overwrite=overwrite_batch)
                time_taken = round(time.time() - now,2)
                print(f"Time Taken to write batch {i+1} to {table_name} = {time_taken}secs")
                print(f"Sent Data to {table_name}")
                
            except Exception as error_message:
                print(f"Connection error {error_message}")
                time.sleep(10)  # wait for 10 seconds then try again
                try:
                    now = time.time()
                    self.session.write_pandas(batch, table_name, parallel=8,schema=schema,database=database,
                                            auto_create_table=auto_create_table,overwrite=overwrite_batch)
                    time_taken = round(time.time() - now,2)
                    print(f"Time Taken to write batch {i+1} to {table_name} = {time_taken}secs")
                    print(f"Sent Data to {table_name}")
                    
                except Exception as error_message:
                    print("Connection failed again")
                    print(f'{table_name} error: ' + str(error_message))
                    return f'{table_name} error: ' + str(error_message) 

        

    def reassert_connection_parameters(self,database,schema):
        '''Function to reassert connection parameters
        
        Args:
            database (str): Name of the database to reassert
            schema (str): Name of the schema to reassert
        
        Returns:
            None'''
        #.collect() is super important
        self.session.sql(f'''USE WAREHOUSE {self.warehouse}''').collect()
        self.session.sql(f'''USE ROLE {self.role}''').collect()
        self.session.sql(f'''USE USER {self.user}''').collect()
        self.session.sql(f'''USE DATABASE {database}''').collect()
        self.session.sql(f'''USE SCHEMA {schema}''').collect()
        
    def select_all_snowflake_view(self,database=None, schema=None, view_name="vw_echopark_monthly_summary",print_previews=False):
        '''Function to read all data from a Snowflake view
        
        Args:
            database (str, optional): Name of the database to read the view from. Defaults to the database specified on class initialisation.
            schema (str, optional): Name of the schema to read the view from. Defaults to the schema specified on class initialisation.
            view_name (str, optional): Name of the view to read. Defaults to "vw_echopark_monthly_summary".
            print_previews (bool, optional): If True, prints the first 5 rows of the view. Defaults to False.
        
        Returns:
            dataframe: Pandas dataframe of the view'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        try:
            #Reassert connection parameters to ensure reliabilty
            self.reassert_connection_parameters(database,schema)
            ##self.logger.info(f'''Reading view {view_name}...''')
            print(f'''Reading view {view_name}...''')
            data = self.session.sql(f'''SELECT * FROM "{database}"."{schema}"."{view_name}"''').to_pandas()
        except Exception as error_message:
            #self.logger.info(f"Connection error {error_message}")
            print(f'{view_name} error: ' + error_message)
            time.sleep(10)
            try:
                #self.logger.info(f'''Reading view {view_name}...''')
                data = self.session.sql(f'''SELECT * FROM "{database}"."{schema}"."{view_name}"''').to_pandas()
            except Exception as error_message:
                #self.logger.error(f'Connection failed again {error_message}',exc_info=True)
                print(f'{view_name} error: ' + error_message)
                return f'{view_name} error: ' + error_message
        
        # Reindex dataframe
        data.reset_index(inplace=True)
        if print_previews:
            self.logger.info(f'''Preview of data:\n''')
            self.logger.info(data.head(10))
        #self.logger.info(f'''Reading view {view_name} completed''')
        print(f'''Reading view {view_name} completed''')
        return data
# %%
