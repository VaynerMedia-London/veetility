import requests
import json
import pandas as pd
import regex as re
import numpy as np
from datetime import datetime,timedelta
import time
from fuzzywuzzy import fuzz
from tqdm.auto import tqdm
import psycopg2 as pg
import sys
import logging
import gspread
import pickle
import gspread_dataframe as gd
import os
import sqlalchemy as sa

emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

logger = logging.getLogger('UtilityFunctions')
if logger.hasHandlers():
    logger.handlers = []
if os.path.isdir('logs') == False:
    os.mkdir('logs')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(levelname)s %(asctime)s - %(message)s')

file_handler = logging.FileHandler(f'./logs/UtilityFunctions.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class UtilityFunctions():

    def __init__(self, gspread_auth_dict=None,db_user=None,db_password=None,db_host=None,
                        db_port=None,db_name=None):
        """Initialise a google sheets connector and postgreSQL connector for the utility instance

        This means you can only connect to one google account and one database per instance 
        of the UtilityFunctions class
        
        Args:
            gspread_auth_dict (dict): A dictionary containing google authorisation data
            db_user (str): The postgreSQL database username
            db_password (str): The postgreSQL database password
            db_host (str): The postgreSQL database host url
            db_port (str): The postgreSQL database port number as a string, usually 5432
            db_name (str): The postgreSQL database name"""
        if gspread_auth_dict != None:
            self.sa = gspread.service_account_from_dict(gspread_auth_dict)         
        if db_user != None:
            self.db_user, self.db_password, self.db_host, self.db_port, self.db_name = \
            db_user, db_password, db_host, db_port, db_name
            postgres_str = f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'
            self.postgresql_engine = sa.create_engine(postgres_str)
        #if root_path != None:
        if os.path.isdir('logs') == False:
            os.mkdir('logs')
        logger = logging.getLogger('UtilLog')
        if logger.hasHandlers():
            logger.handlers = []
        # if os.path.isdir(f'{root_path}/logs') == False:
        #     os.mkdir(f'{root_path}/logs')
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(sys.stdout))
        formatter = logging.Formatter('%(levelname)s %(asctime)s - %(message)s')
        #file_handler = logging.FileHandler(f'{root_path}/logs/UtilLog.log')
        file_handler = logging.FileHandler(f'./logs/UtilLog.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        self.logger = logger
    
    def prepare_string_matching(string, is_url=False):
        """Prepare strings for matching say in a merge function by removing unnecessary 
            detail, whitespaces and converting to lower case
            Remove URLs and emojis as sometimes they cannot come through properly in Tracer data
            Replace non-ASCII characters with their closest ASCII equivalents

            Parameters
            -----------------
            string : str 
                The string to be cleaned
            is_url : bool 
                If True then remove URLs and characters after the '?' which are utm parameters
                These can be present in some URLs we receive and not others
            Returns 
            ----------------
            string : str
                A cleaned string stripped of whitespace, punctuation, emojis, non-ASCII characters, and URLs.
        """
        string = string.lower()
        if is_url:
            # Remove URLs and characters after the '?'
            string = string.split('?')[0] # get rid of everything after they start to be utm parameters

        else: #this is commonly for a post message
            string = string + ' ' # add a space to the end of the string so that the regex below works            
            string = re.sub(r'(https?://\S+)\s', '', string) # remove URLs up to the first whitespace

        string = emoji_pattern.sub(r'', string) # remove emojis
        string = unidecode(string)  # replace non-ASCII characters with their closest ASCII equivalents
        string = re.sub(r'[^\w\s]', '', string) # remove punctuation
        return string.replace(' ', '')

    def match_ads(self,df_1, df_2, df_1_exact_col, df_2_exact_col,
                df_1_fuzzy_col=None, df_2_fuzzy_col=None, is_exact_col_link=True, 
                matched_col_name='boosted', merge=False,cols_to_merge =['platform'],pickle_name='NoStore'):

        """Match row items in df_2 onto row items in df_1 based on two sets of columns, first the dataframes will be tried to match using the first set of columns using an exact match
        
        Then for the row items that haven't matched then use the second set of columns and try to match them 
        using fuzzy matching
        
        Args:
            df_1 (DataFrame): The Dataframe that will be searched to see if any corresponding values in df_2, if merge=True df_2 will be left joined onto df_1
            df_2 (DataFrame): The Dataframe that if merge = True will be left joined onto df_1
            df_1_exact_col (str): The Column name from df_1 that will be first attempted to find exact matches
            df_2_exact_col (str): The Column name from df_2 that will be first attempted to find exact matchesq
            df_1_fuzzy_col (str): The Column name from df_1 that will be attempted to fuzzy match if there was no exact match before.
            df_2_fuzzy_col (str): The Column name from df_2 that will be attempted to fuzzy match if there was no exact match before.
            is_exact_col_link (bool): Boolean Flag, is the set of columns to be exact matched hyperlinks? If so they will be cleaned to remove utm parameters.
            matched_col_name (str): String to name the column which will contain boolean values to indicate whether row items in df_1 found a match in df_2.
            merge (bool): Boolean Flag, if true then df_2 will be left joined onto df_1. Else df_1 will be left unchanged apart from column indicating whether there is a match.
            cols_to_merge (list, str): List of strings to merge on if 'merge' = True.
            pickle_name (str): Name of the dictionary of best matches found by fuzzy matching to be stored as a pickle file. The next time the function is run with the same pickle_name, the pickle file is used to find matches without having to do slow fuzzy matching from scratch.
            
        Returns:
            df_1 (DataFrame): The original df_1 with just a column to indicate whether a match has occured if merge = False else df_1 will have df_2 left joined on.
            df_2 (DataFrame): The original df_2 with cleaned columns and 'Match String' column to help quality check why some rows have or haven't matched.
            df_2_no_match (DataFrame): A dataframe of df_2 row items that haven't found a match in df_1.
            """
                
        df_1_num_rows = df_1.shape[0] #Used later to check we don't lose of rows by merging
        if df_1_fuzzy_col == None:
            df_1_fuzzy_col = df_1_exact_col
            df_2_fuzzy_col = df_2_exact_col

        #create new columns for the columns to match on, the text in the column will get cleaned later
        df_1_fuzzy_col_clean = df_1_fuzzy_col + '_clean'
        df_2_fuzzy_col_clean = df_2_fuzzy_col + '_clean'
        df_1_exact_col_clean = df_1_exact_col + '_clean'
        df_2_exact_col_clean = df_2_exact_col + '_clean'

        df_1[df_1_fuzzy_col] = df_1[df_1_fuzzy_col].astype(str)
        df_2[df_2_fuzzy_col] = df_2[df_2_fuzzy_col].astype(str)
        df_1[df_1_exact_col] = df_1[df_1_exact_col].astype(str)
        df_2[df_2_exact_col] = df_2[df_2_exact_col].astype(str)

        df_1['message'] = df_1['message'].replace('None','NoValuePresent') # this stops multiple none values in df_2 being written onto ever y null value in df_1

        # prepare strings in a raw form with no spaces or punctuation in order to increase chance of matching
        df_1['match_string'] = df_1[df_1_exact_col].apply(lambda x: self.prepare_string_matching(x, is_url=is_exact_col_link))
        df_2['match_string'] = df_2[df_2_exact_col].apply(lambda x: self.prepare_string_matching(x, is_url=is_exact_col_link))
        df_1[df_1_fuzzy_col_clean] = df_1[df_1_fuzzy_col].apply(lambda x: self.prepare_string_matching(x))
        df_2[df_2_fuzzy_col_clean] = df_2[df_2_fuzzy_col].apply(lambda x: self.prepare_string_matching(x))

        #find out the number of unique values of the first cleaned column to match by
        df_2_unique_exact = df_2['match_string'].unique().tolist()
        df_1_unique_exact = df_1['match_string'].unique().tolist()

        #find out whether there is an exact match on the first cleaned column by using the list of unique values from df_2
        df_1['matched_exact?'] = df_1['match_string'].apply(lambda x: True if x in df_2_unique_exact else False)
        df_1[matched_col_name] = False
        df_1['matched_fuzzy?'] = False
        # Split Dataframes up into rows that had a URL match and one where the rows didn't match
        # Then the ones that didn't match will try and be matched with the cleaned caption
        df_1_match = df_1[df_1['matched_exact?'] == True]
        df_1_no_match = df_1[df_1['matched_exact?'] == False]

        df_1_match_rows = df_1_match.shape[0] #this is used to find out the change in the number of rows of the left df after the merge
        cols_to_merge.append('match_string') #from the cols_to_merge specified as an input argument add the matchstring
        if merge:
            df_1_match = pd.merge(df_1_match, df_2.drop(df_2_fuzzy_col_clean, axis=1),
                                left_on=cols_to_merge, right_on=cols_to_merge, how='left',indicator=True)
            #Indicate whether a post has matched using the exact column (the first merge) if the result of the indcator is "both"
            df_1_match['matched_exact?'] = df_1_match['_merge'].apply(lambda x: True if x == 'both' else False)
        else:
            df_1['matched_exact?'] = df_1['match_string'].apply(lambda x: True if x in df_2_unique_exact else False)
        
        logger.info(f'Rows count change after df_1_match merge {df_1_match.shape[0] - df_1_match_rows}')
        # Find the unique instances of cleaned captions to be passed to the fuzzy matching function
        df_1_no_match_unique = df_1_no_match[df_1_fuzzy_col_clean].unique().tolist()
        df_2_fuzzy_unique = df_2[df_2_fuzzy_col_clean].unique().tolist()

        # the fuzzy match function will return a dictionary of matches for each caption from df_1 that wasn't
        # matched by link and a list of captions from df_2 that didn't match
        best_match_dict = self.best_fuzzy_match(df_1_no_match_unique, df_2_fuzzy_unique, 90,pickle_name)

        # create a column that is the closest match in df_2 for every caption in df_1
        # This will be used to merge df_2 onto the remainder of none matching df_1
        df_1_no_match['match_string'] = df_1_no_match[df_1_fuzzy_col_clean].map(best_match_dict)

        df_1_no_match['match_string'].fillna(False, inplace=True)
        #Idnetify whether a post has matched using the fuzzy matching function by whether there is a value there
        df_1_no_match['matched_fuzzy?'] = df_1_no_match['match_string'].apply(lambda x: False if ((x == 'None') or (x == '')) else True)
        df_2['match_string'] = df_2.apply(lambda x: x['match_string'] if x['match_string'] in df_1_unique_exact else x[df_2_fuzzy_col_clean], axis=1)

        #Now try and merge the rows that didn't match on the exact column
        df_1_no_match_num_rows = df_1_no_match.shape[0]
        if merge:
            df_1_no_match = pd.merge(df_1_no_match, df_2.drop(df_2_fuzzy_col, axis=1), left_on=cols_to_merge,
                                    right_on=cols_to_merge, how='left',indicator=True)
        
            df_1_no_match['matched_fuzzy?'] = df_1_no_match['_merge'].apply(lambda x: True if x == 'both' else False)
        

        logger.info(f'Rows Lost after no match merge {df_1_no_match_num_rows - df_1_no_match.shape[0]}')

        df_1 = pd.concat([df_1_match, df_1_no_match], ignore_index=True)

        df_1[matched_col_name] = df_1.apply(lambda x: True if ((x['matched_exact?'] == True) or (x['matched_fuzzy?'] == True)) else False, axis=1)

        df_2_no_match = df_2[~df_2['match_string'].isin(df_1['match_string'].unique().tolist())]

        logger.info(f'df_1 row numbers change = {df_1_num_rows - df_1.shape[0]}')

        logger.info(f"Num unique exact col values in df_1: {df_1[df_1_exact_col].nunique()},\
                Num unique fuzzy col values in df_1: {df_1[df_1_fuzzy_col].nunique()}")
        logger.info(f"Num unique exact col values in df_2: {df_2[df_2_exact_col].nunique()},\
                Num unique fuzzy col values in df_2: {df_2[df_2_fuzzy_col].nunique()}")

        logger.info(f"Num Matched df_1 exact col ={df_1['matched_exact?'].sum()}")
        logger.info(f"Num Matched df_1 fuzzy col ={df_1['matched_fuzzy?'].sum()}")
        logger.info(f"Num df_2 exact that didn't match= {df_2_no_match[df_2_exact_col].nunique()}")

        matched_df_1_nunique = df_1[df_1[matched_col_name]==True].drop_duplicates(subset=cols_to_merge).shape[0]
        logger.info(f"Number of df_1 that have matched = {matched_df_1_nunique}")
        num_df_2_to_match = df_2.drop_duplicates(subset=cols_to_merge).shape[0]
        logger.info(f"Number of df_2 that need to match {num_df_2_to_match}")
        logger.info(f"Percentage of df_2 that were matched = {round((matched_df_1_nunique * 100)/num_df_2_to_match,2)}")

        return df_1, df_2, df_2_no_match

    def best_fuzzy_match(self,list_1, list_2, threshold, pickle_name):
        """Takes in two lists of strings and every string in list_1 is fuzzy matched onto every item in list_2
        The fuzzy match of a string in list_1 with a string in list_2 with the highest score will count as the 
        match as long as it is above the threshold. The match is then stored as a key value pair in a dictionary

        The dictionary of matches will be saved as a pickle file to be used next time the function is run to save
        having to do searches on a string if we've already found a match in the past

        Args:
            list_1 (list): First List of strings, every item will be searched for a fuzzy match in list_2
            list_2 (list): Second List of strings, every item in list_1 will be fuzzy matched with every item in list 2 and best fuzzy match score wins
            threshold (integer): value between 0 and 100 signifying percentage fuzzy match score at which a match is considered sufficiently close
            pickle_name (str): name of pickle_file to create or add to if a pickle of the dictionary already exists
        
        Returns:
            best_match_dict (dict): Dictionary of matches (with highest fuzzy match score) between strings in list_1 and list_2 
                    key = string in list_1, value = string in list_2 """

        best_match_dict = {} 
        stored_best_dict = {} #
        if pickle_name != 'NoStore':
            if os.path.isfile(f'Pickled Files/best_match_dict_{pickle_name}'):
                stored_best_dict = self.unpickle_data(f'best_match_dict_{pickle_name}')
                logger.info(f"loaded dict of len :{len(stored_best_dict)}")

        for string_1 in tqdm(list_1):
            
            if string_1 == '':
                best_match_dict[''] = 'None'
                continue
            # If there is an exact match then just put the match as itself and no need to go through list
            if string_1 in list_2:  
                best_match_dict[string_1] = string_1
                continue
            
            #If there is a match in the stored dictionary then use that
            if string_1 in list(stored_best_dict.keys()): 
                best_match_dict[string_1] = stored_best_dict[string_1]
                continue
            
            #Then go through every string in the second list and fuzzy match to produce a score
            #If the score is below the threshold then set the score to 0
            #start = time.time()
            temp_match_dict = {}
            for string_2 in list_2:
                score = fuzz.ratio(string_1, string_2)
                if score < threshold:
                    score = 0
                temp_match_dict[string_2] = score
            # end = time.time()
            # logger.info(end - start)
            
            #if there were no matches above the threshold then return a match for that 
            #string in list_1 equal to "none"
            if max(temp_match_dict.values()) == 0:
                best_match_dict[string_1] = 'None'
            else:
                # find the match with the highest matching score and save that to the
                # best match dictionary
                best_match = max(temp_match_dict.items(), key=lambda k: k[1])[0]
                best_match_dict[string_1] = best_match

        if pickle_name!= 'NoStore':
            # Remove matches that didn't find anythign as that will let new values be discovered
            # if new data comes in
            #best_match_dict_none_removed = {k:v for k,v in best_match_dict.items() if v != 'None'}
            self.pickle_data(best_match_dict,f'best_match_dict_{pickle_name}')

        return best_match_dict

    def write_to_postgresql(self,df,table_name, if_exists='replace'):
        """Writes a dataframe to a PostgreSQL database table using a SQLalchemy engine defined elsewhere.
        If writing fails it waits 10 seconds then trys again
            
        Args:
            df (DataFrame): The Dataframe to send to the PostGreSQL table
            table_name (str): The name of the table to write the dataframe to
            if_exists (str): Either 'replace' or 'append' which describes what to do if a table with that name already exists
                                    
        Returns:
            error_message (str): An error message saying that the connection has failed """
        # create a SQL alchemy engine to write the data to the database after cleaning
        
        error_message = ''
        try:
            now = time.time()
            df.to_sql(table_name, con=self.postgresql_engine, index=False, if_exists=if_exists)
            time_taken = round(time.time() - now,2)
            logger.info(f"Time Taken to write {table_name} = {time_taken}secs")
            logger.info(f"Sent Data to {table_name}")
        except ConnectionError as error_message:
            logger.info(f"Connection error {error_message}")
            time.sleep(10) # wait for 10 seconds then try again
            try:
                now = time.time()
                df.to_sql(table_name, con=self.postgresql_engine, index=False, if_exists=if_exists)
                time_taken = round(time.time() - now,2)
                logger.info(f"Time Taken to write {table_name} = {time_taken}secs")
            except Exception as error_message:
                logger.error(f'Connection failed again {error_message}',exc_info=True)
                return f'{table_name} error: ' + error_message
        return error_message
    
    def table_exists(self, table_name):
        """Check if a table with the given name exists in the database.

        Args:
            table_name (str): name of the table to check for existence.

        Returns:
            bool: True if table exists, False otherwise."""
        all_table_names = sa.inspect(self.postgresql_engine).get_table_names()
        return (table_name in all_table_names)

    
    def store_daily_organic_data(self,df,output_table_name,num_days_to_store=30,date_column_name='date',
                                    check_created_col=True,created_col='created',refresh_lag=1):
        """Takes in an organic data table with each row item reflecting an organic post with the metric totals
        updating and increasing in the same row item each day rather than creating a new row item each day.
        Returns an organic table which has a row item  
            
        Args:
            df (DataFrame): The Dataframe source of organic data
            output_table (str): The name of the output table to write the data to
            num_days_to_store (int): The number of days to look back
            date_column_name (str): The name of the column which contains the date that the post was originally posted
            check_created_col (bool): If true then we should check whether the created column in Tracer is up to date, because 
                there is no point sending a days worth of data if the row items have not been refreshed
            created_col (str): The name of the column in the dataset which indicates the date that it was last updated
       
        Returns:
            df (DataFrame): Outputs a table to the 'output_table_name', appends if already exists or creates from scratch if not"""

        today_datetime = datetime.today()
        today_date = today_datetime.date() 
        if today_datetime.hour < 15: #Before 3 o'clock in the afternoon
            logger.info("It may be better to run the API later on in the day to make sure the USA data has had time to refresh")
        #Check Tracer data has actually updated
        if self.table_exists(output_table_name):

            old_df = self.read_from_postgresql(output_table_name)
            if old_df['date_updated'].max().date() == today_date:
                logger.info(f"It looks data has already pushed to {output_table_name} today")
            else:
                if (df[created_col].max().date() < today_date - timedelta(days=refresh_lag)) and (check_created_col):
                    error_message = "It looks like the input df has not been updated yesterday. Therefore there is no fresh data to add on."
                    logger.info(error_message)
                    raise Exception(error_message)
                cutoff_date = today_date - timedelta(days=num_days_to_store)
                #create temporary date column that you can change the date format, that you then delete so it doesn't affect original date format
                df['datetemp'] = pd.to_datetime(df[date_column_name]).dt.date
                df = df[df['datetemp']>=(cutoff_date)]#filter data only after the cutoff date
                df['date_updated'] = today_datetime
                df = df.drop(columns=['datetemp']) #drop temporary date column used for filtering dates
                self.write_to_postgresql(df,output_table_name,if_exists='append')
        else: #if the table doesn't exist create it with the whole dataset for the first time
            df['date_updated'] = today_datetime
            self.write_to_postgresql(df,output_table_name,if_exists='replace')


    def read_from_postgresql(self, table_name, clean_date=True, date_col='EnterValue', dayfirst='EnterValue', yearfirst='EnterValue', format=None, errors='raise'):
        """Reads a table from a PostgreSQL database table using a pscopg2 connection.
        If fails it waits 10 seconds and tries again.
        
        Args:
            table_name (str): The name of the table to read from.
            clean_date (bool, optional): Whether or not to clean the date column. Defaults to True.
            date_col (str, optional): The column name of the date column to clean. Defaults to 'EnterValue'.
            dayfirst (str, optional): The day first format for date parsing. Defaults to 'EnterValue'.
            yearfirst (str, optional): The year first format for date parsing. Defaults to 'EnterValue'.
            format (str, optional): The format for date parsing. Defaults to None.
            errors (str, optional): The behavior for date parsing errors. Defaults to 'raise'.

        Returns:
            df (pandas.DataFrame): The table data in a pandas dataframe.
        """
        # Connect to the PostgreSQL database
        conn = pg.connect(dbname=self.db_name, host=self.db_host,
                    port=self.db_port, user=self.db_user, password=self.db_password)
        try:
            # Try to read the table
            now = time.time()
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            time_taken = round(time.time() - now,2)
            logger.info(f"Time Taken to read {table_name} = {time_taken}secs")

        except Exception as error_message:
            # If reading the table fails, log the error and wait 10 seconds before trying again
            logger.error(f"Read {table_name} error: {error_message}",exc_info=True)
            time.sleep(10)

            now = time.time()
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            time_taken = round(time.time() - now,2)
            logger.info(f"Time Taken to read {table_name} = {time_taken}secs")

        # Close the database connection
        conn.close()
        
        # If specified, clean the date column
        if clean_date:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, yearfirst=yearfirst,
                                        format=format, errors=errors)
        return df


    def write_to_gsheet(self, workbook_name, sheet_name, df, if_exists='replace', sheet_prefix=''):
        """Write a dataframe to a google sheet

        Args:
            workbook_name (str): The name of the google sheet workbook.
            sheet_name (str): The name of the sheet to write data to.
            df (pandas.DataFrame): The dataframe to be written to the google sheet.
            if_exists (str, optional): Determines the behavior when the sheet already exists, options are 'replace' or 'append'. (default='replace')
            sheet_prefix (str, optional): A prefix to be added to the sheet name. (default='')

        Returns:
            None    """
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        df['SheetUpdated'] = dt_string
        try:
            # Adding the sheet prefix to the sheet name
            sheet_name = sheet_prefix + sheet_name
            # Open the worksheet
            sheet = self.sa.open(workbook_name).worksheet(sheet_name)
            if if_exists == 'replace': 
                # Clear the sheet if if_exists is set to 'replace'
                sheet.clear()
            now = time.time()
            # Write the dataframe to the sheet
            gd.set_with_dataframe(sheet, df)
            time_taken = round(time.time() - now,2)
            logger.info(f"Time Taken to write to google sheet {sheet_name} = {time_taken}secs")
        except Exception as error_message:
            logger.error(error_message,exc_info=True)
            time.sleep(10)
            gd.set_with_dataframe(sheet, df)



    def read_from_gsheet(self,workbook_name, sheet_name,clean_date=True,date_col='EnterValue',
                                dayfirst='EnterValue',yearfirst='EnterValue',format=None,errors='raise'):
        """Read data from a google sheet and return it as a dataframe.

        Parameters:
            workbook_name (str): The name of the google sheet workbook.
            sheet_name (str): The name of the sheet to read data from.
            clean_date (bool): If true, the 'date_col' column will be converted to datetime format. (default: True)
            date_col (str): The name of the column containing the date values to be cleaned. (default: 'EnterValue')
            dayfirst (bool): Whether to interpret the first value in an ambiguous 3-integer date (e.g. 01/05/09) as the day (True) or month (False). (default: 'EnterValue')
            yearfirst (bool): Similar to 'dayfirst', but for the year. (default: 'EnterValue')
            format (str): The format of the date values. (default: None)
            errors (str): The behavior when encountering errors in the date format. (default: 'raise')

        Returns:
            df (pandas.DataFrame): The dataframe containing the data from the google sheets"""
        try:
            spreadsheet = self.sa.open(workbook_name)
        except Exception as error_message:
            logger.info(error_message)
            time.sleep(10)
            spreadsheet = self.sa.open(workbook_name)
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        if clean_date:
            df[date_col] = pd.to_datetime(df[date_col],dayfirst=dayfirst,yearfirst=yearfirst,
                                            format=format,errors=errors)
        return df

    
    def identify_paid_or_organic(self,df):
        """Identify whether a given dataframe contains paid data"""
        paid_or_organic = 'Organic'
        #make columns to check lower case so can work on
        # columns that have or haven't been cleaned
        col_list = [x.lower() for x in df.columns]
        if 'spend' in col_list:
            paid_or_organic = 'Paid'
        return paid_or_organic

    def pickle_data(self,data, filename,folder="Pickled Files"):
        """Pickle data and save it to a file.
        
        Args:
            data (Object): The data to be pickled.
            filename (str): The name of the file to save the pickled data to.
            folder (str, optional): The folder to save the pickled file to. Defaults to "Pickled Files"."""

        if os.path.isdir(folder) == False:
            os.mkdir(folder)
        pickle.dump(data, open(folder + '/' + filename, "wb"))

    
    def unpickle_data(self,filename,folder ="Pickled Files"):
        """Load pickled data from a file.
        
        Args:
            filename (str): The name of the file to load the pickled data from.
            folder (str, optional): The folder where the pickled file is located. Defaults to "Pickled Files".
        
        Returns:
            Object: The unpickled data"""
        return pickle.load(open(folder + '/' + filename, "rb"))

    
    def write_json(self,object,file_name,file_type):
        """Write a Python object to a json file.
        
        Args:
            object (Object): The Python object to be written to a json file.
            file_name (str): The name of the json file to be created.
            file_type (str): The type of the object. It must be 'DataFrame', 'List' or 'Dictionary'
        """
        if file_type == 'DataFrame':
            object.to_json(file_name+'.json',orient='split')
        elif file_type == 'List' or file_type == 'Dictionary':
            with open(f"{file_name}.json","w") as outfile:
                json.dump(object,outfile)
        else:
            logger.error('JSON write error, file_type error')
    
    def read_json(self,file_name, file_type):
        """Read a json file and return a Python object.
        
        Args:
            file_name (str): The name of the json file to be read.
            file_type (str): The type of the object. It must be 'DataFrame', 'List' or 'Dictionary'
        
        Returns:
            Object: The object read from json file."""
        if file_type == 'DataFrame':
            return pd.read_json(f'{file_name}.json',orient='split')
        elif file_type == 'List' or file_type == 'Dictionary':
            return json.load(open(f'{file_name}.json'))
        else:
            logger.error('JSON read error, file_type error')


    # def remove_vvm_stage(self,creative_name):
    #     creative_name = re.sub(r'Level 2 - ','', creative_name)
    #     creative_name = re.sub(r'Level2 -','', creative_name)
    #     creative_name = re.sub(r'Level_2-','', creative_name)
    #     creative_name = re.sub(r'Level2_','', creative_name)
    #     return creative_name

    # def calc_tiktok_vtr_rates(self,df):
    #     df['Engagements'] = df['likes'] + df['comments'] + df['shares']
    #     df['EngagementRate'] = round(
    #         df['Engagements'] * 100 / df['impressions'], 2)
    #     df['25%VTR'] = round(df['Video Views P 25'] * 100 / df['impressions'], 2)
    #     df['50%VTR'] = round(df['Video Views P 50'] * 100 / df['impressions'], 2)
    #     df['75%VTR'] = round(df['Video Views P 75'] * 100 / df['impressions'], 2)
    #     df['100%VTR'] = round(df['Video Completions'] * 100 / df['impressions'], 2)
    #     return df
    
    def columnnames_to_lowercase(self,df):
        """Change the columns in a dataframe into lowercase with spaces replaced by underscores"""
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ','_')
        df.columns = df.columns.str.strip()
        return df


def merge_w_match_perc(self,df_1,df_2,left_on,right_on,how='left'):
        """Merges two dataframes and prints out the number of matches and the percentage of matches out of the total number of rows.
        
        Args:
            df_1 (pd.DataFrame): The first dataframe to merge
            df_2 (pd.DataFrame): The second dataframe to merge
            left_on (str): The column name to merge on in the first dataframe
            right_on (str): The column name to merge on in the second dataframe
            how (str, optional): The type of merge to perform. Defaults to 'left'.
        
        Returns:
            output_df (pd.DataFrame): A pandas dataframe that contains the merged data from the input dataframes."""
        df_1_rows = df_1.shape[0]
        if '_merge' in df_1.columns:
            df_1 = df_1.drop('_merge',axis=1)

        output_df = pd.merge(df_1,df_2,left_on=left_on,right_on=right_on,how=how,indicator=True)
        num_rows = output_df.shape[0]
        num_matches = output_df._merge.value_counts()['both']
        match_perc = round(num_matches / num_rows * 100,1)
        rows_diff = df_1_rows - num_rows
        print(f"df_1 has {df_1_rows} rows")
        print(f'{num_matches} matches out of {num_rows} rows ({match_perc}%)')
        print(f'{rows_diff} ')
        return output_df

    # def group_by_asset(self,x):
    #     d = {}

    #     d['Engagements'] = x['Engagements'].sum()
    #     d['impressions'] = x['impressions'].sum()
    #     d['EngagementRate'] = round(d['Engagements'] * 100 / d['impressions'], 2)
    #     d['25%VTR'] = round(x['25%VTR'].mean(), 2)
    #     d['50%VTR'] = round(x['50%VTR'].mean(), 2)
    #     d['75%VTR'] = round(x['75%VTR'].mean(), 2)
    #     d['100%VTR'] = round(x['100%VTR'].mean(), 2)
    #     d['Video Length'] = round(x['Video Length'].iloc[0], 2)

    #     return pd.Series(d, index=list(d.keys()))

class Logger:
    """A class for handling logging of events.
    
    Args:
        name_of_log (str): The name of the log file"""

    def __init__(self,name_of_log):
        """Initialize the logger.
        
        Args:
            name_of_log (str): The name of the log file."""
        self.name_of_log = name_of_log
        logger = logging.getLogger(__name__)
        if logger.hasHandlers():
            logger.handlers = []
        if os.path.isdir('logs') == False:
            os.mkdir('logs')
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(sys.stdout))
        formatter = logging.Formatter('%(levelname)s %(asctime)s - %(message)s')

        file_handler = logging.FileHandler(f'./logs/{name_of_log}.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        self.logger = logger

class SlackNotifier:
    """A class that sends a message to a slack channel given a webhook url and optional url links
    Args:
        slack_webhook_url (str): The webhook url for the slack channel
        title (str): The title of the slack message. Default: "Update"
        link_1 (str): An optional link to be included in the slack message. Default: ''
        link_1_name (str): The name to be displayed for link_1. Default: 'No Url'
        link_2 (str): An optional link to be included in the slack message. Default: ''
        link_2_name (str): The name to be displayed for link_2. Default: 'No Url'
        link_3 (str): An optional link to be included in the slack message. Default: ''
        link_3_name (str): The name to be displayed for link_3. Default: 'No Url'
        link_4 (str): An optional link to be included in the slack message. Default: ''
        link_4_name (str): The name to be displayed for link_4. Default: 'No Url'"""

    def __init__(self, slack_webhook_url: str, title = "Update" ,link_1 = '', link_1_name='No Url',
                    link_2 ='', link_2_name = 'No Url', link_3 = '', link_3_name = 'No Url',
                    link_4='', link_4_name='No Url'):
        """Initiate slack notifier object

        Args:
            slack_webhook_url (str): The webhook url for the slack channel
            title (str): The title of the slack message. Default: "Update"
            link_1 (str): An optional link to be included in the slack message. Default: ''
            link_1_name (str): The name to be displayed for link_1. Default: 'No Url'
            link_2 (str): An optional link to be included in the slack message. Default: ''
            link_2_name (str): The name to be displayed for link_2. Default: 'No Url'
            link_3 (str): An optional link to be included in the slack message. Default: ''
            link_3_name (str): The name to be displayed for link_3. Default: 'No Url'
            link_4 (str): An optional link to be included in the slack message. Default: ''
            link_4_name (str): The name to be displayed for link_4. Default: 'No Url'"""
    
        self.slack_webhook_url, self.title = slack_webhook_url, title
        self.link_1,self.link_2,self.link_3,self.link_4 = link_1,link_2,link_3,link_4
        self.link_1_name,self.link_2_name,self.link_3_name,self.link_4_name = link_1_name,link_2_name,link_3_name,link_4_name
        
    def send_slack_message(self, message:str):
        """Sends message to slack channel
        Args:
            message (str): the message to be sent to the slack channel"""

        payload = {
        "blocks":
        [
            {
                  "type": "header",
                  "text": {
                      "type": "plain_text",
                      "text": f"{self.title}",
                        }
            },

            {
                  "type": "divider"
            },

            {
                  "type": "section",
                  "fields": [
                      {
                          "type": "mrkdwn",
                          "text": message
                      },

                    ],
            },

            {
                  "type": "divider"
            },

            {
                  "type": "section",
                  "text": {
                      "type": "mrkdwn",
                      "text": f"<{self.link_1}|{self.link_1_name}>"
                  }
            },

            {
                  "type": "section",
                  "text": {
                      "type": "mrkdwn",
                      "text": f"<{self.link_2}|View {self.link_2_name}>"
                        }
            },

            {
                  "type": "section",
                  "text": {
                      "type": "mrkdwn",
                      "text": f"<{self.link_3}|View {self.link_3_name}>"
                        }
            },

            {
                  "type": "section",
                  "text": {
                      "type": "mrkdwn",
                      "text": f"<{self.link_4}|View {self.link_4_name}>"
                        }
            },

        ]
        }
        try:
            requests.post(self.slack_webhook_url, data=json.dumps(payload))
        except Exception as e:
            logger.error(f"Failed To Send Slack messafe {e}",exc_info=True)
