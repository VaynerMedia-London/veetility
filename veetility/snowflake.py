#%%
#import env
import pandas as pd
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL
from snowflake.snowpark import Session
from snowflake.sqlalchemy import URL
from datetime import datetime
import time
from sqlalchemy import create_engine
#import utility_functions as uf
#%%


#%%
class Snowflake():
    
    def __init__(self,connection_params_dict,client_name) -> None:
        
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
        print(self.session.sql('''SELECT 'Connected!' as STATUS''').collect())
        # Show databases
        df_test = self.session.sql('SHOW DATABASES')
        df_test.show()
        # Show roles
        #self.logger.info(self.session.sql(f'''USE ROLE {self.role}''').collect())
    
    def get_data_with_URL(self,data_table, schema=None):
        """Function to read Snowflake table using SQLAlchemy
        
        URL stands for Uniform Resource Locator. It is a reference (an address) to a resource on the Internet."""

        if schema == None:
            schema = self.schema

        url = URL(user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=schema,
                role = self.role)
    
        engine = create_engine(url)
        connection = engine.connect()

        query_data = '''select * from "{}" '''.format(data_table)

        df = pd.read_sql(query_data, connection)
        df.info
        return df  

    def create_table_from_dataframe(self,df,table_name,database=None, schema=None):
        '''Define function to create Snowflake table from Pandas dataframe'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        # Create table
        self.session.createDataFrame(df).createOrReplaceTempView(table_name)
        # Create table in Snowflake
        self.session.sql(f'''CREATE OR REPLACE TABLE "{database}"."{schema}"."{table_name}" AS SELECT * FROM {table_name}''').collect()
    
    def drop_table(self,table_name,database=None, schema=None):
        '''Function to drop Snowflake table'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        # Drop table
        self.session.sql(f'''DROP TABLE "{database}"."{schema}"."{table_name}"''').collect()
    
    def write_df_to_snowflake(self,df,table_name,database=None,schema=None,
                              auto_create_table=False,overwrite=False):
        '''Truncates (if it exists) or creates new table and inserts the new data into the selcted table'''
        now = time.time()

        if schema ==None:
            schema = self.default_schema
        if database == None:
            database = self.default_database
        try:
            #Reassert connection parameters to ensure reliabilty
            self.reassert_connection_parameters(database,schema)
            self.session.write_pandas(df, table_name, parallel=8,schema=schema,database=database,
                                      auto_create_table=auto_create_table,overwrite=overwrite)
            time_taken = round(time.time() - now,2)
            #self.logger.info(f"Time Taken to write {table_name} = {time_taken}secs")
            #self.logger.info(f"Sent Data to {table_name}")
        except Exception as error_message:
            #self.logger.info(f"Connection error {error_message}")
            time.sleep(10) # wait for 10 seconds then try again
            try:
                now = time.time()
                self.session.write_pandas(df, table_name, parallel=8,schema=schema,database=database,
                                      auto_create_table=auto_create_table,overwrite=overwrite)
                time_taken = round(time.time() - now,2)
                #self.logger.info(f"Time Taken to write {table_name} = {time_taken}secs")
                #self.logger.info(f"Sent Data to {table_name}")
            except Exception as error_message:
                print("Connection failed again")
                #self.logger.error(f'Connection failed again {error_message}',exc_info=True)
            return f'{table_name} error: ' + error_message
        return error_message

    def reassert_connection_parameters(self,database,schema):
        '''Function to reassert connection parameters
        This just ensures reliabilty of the connection'''
        #.collect() is super important
        self.session.sql(f'''USE WAREHOUSE {self.warehouse}''').collect()
        self.session.sql(f'''USE ROLE {self.role}''').collect()
        self.session.sql(f'''USE USER {self.user}''').collect()
        self.session.sql(f'''USE DATABASE {database}''').collect()
        self.session.sql(f'''USE SCHEMA {schema}''').collect()
        
    def select_all_snowflake_view(self,database=None, schema=None, view_name="vw_echopark_monthly_summary",print_previews=False):
        '''Define function to read Snowflake view using Snowpark'''
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        try:
            #Reassert connection parameters to ensure reliabilty
            self.reassert_connection_parameters(database,schema)
            ##self.logger.info(f'''Reading view {view_name}...''')
            data = self.session.sql(f'''SELECT * FROM "{database}"."{schema}"."{view_name}"''').to_pandas()
        except Exception as error_message:
            #self.logger.info(f"Connection error {error_message}")
            time.sleep(10)
            try:
                #self.logger.info(f'''Reading view {view_name}...''')
                data = self.session.sql(f'''SELECT * FROM "{database}"."{schema}"."{view_name}"''').to_pandas()
            except Exception as error_message:
                #self.logger.error(f'Connection failed again {error_message}',exc_info=True)
                return f'{view_name} error: ' + error_message
        
        # Reindex dataframe
        data.reset_index(inplace=True)
        if print_previews:
            self.logger.info(f'''Preview of data:\n''')
            self.logger.info(data.head(10))
        #self.logger.info(f'''Reading view {view_name} completed''')
        return data
# %%
