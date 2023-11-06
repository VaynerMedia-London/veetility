import pandas as pd
import regex as re
import numpy as np
from datetime import date, datetime
from tqdm.auto import tqdm
import os
import logging
import sys
today = pd.to_datetime(date.today())

#%%-----------------------------
# Initialise Logger
# ------------------------------

logger = logging.getLogger('QAFunctions')
if logger.hasHandlers():
    logger.handlers = []
if os.path.isdir('logs') == False:
    os.mkdir('logs')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(levelname)s %(asctime)s - %(message)s')

file_handler = logging.FileHandler(f'./logs/QAFunctions.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class QualityAssessments:
    """A class for performing quality assessment functions on social media advertising data.
    
    Examples:
        1. Check the percentage of null or nan values for certain channels of data, e.g. Germany TikTok has 5% of the post messages as null values
        2. Store the historic sum totals of columns such as likes, which should increase linerarly over time as more adverts are run. If a sharp increase or a 
            decrease is detected then raise an error.
        3. Check the percentage of duplicates in a dataframe of advertising data. Automatic detection of whether the data is paid or organic and what attributes of those
            datasets would normally constitute a duplicate or not. 

    It outputs results to Google Sheets and can raise exceptions or send notifications when issues are detected.

    Attributes:
        util: A utility object (from utility_functions in this library) for writing results to google sheets and databases."""

    def __init__(self, util_object=None):

        if util_object != None:
            self.util = util_object
    
    def null_values_checker(
            self,
            df,
            cols_to_group,
            gsheet_name,
            tab_name,
            cols_to_ignore=None,
            null_definitions=None,
            output_method='gsheet'
    ):
        """Takes in a dataframe and columns to groupby and checks how many null values or null equivalents
        there are in the rest of the columns.
            
        Args:
            df (pandas.DataFrame): The Input Dataframe of any type where you want to check for nulls.
            cols_to_group (list of str): The list of columns to perform a groupby operation with the null 
                                        percentage counts will be a percentage of nulls in these groupbys.
            cols_to_ignore (list of str): The list of columns to ignore when checking for nulls. Default is None.
            gsheet_name (str): The name of the google sheet workbook to pass to the google sheet function.
            tab_name (str): The name of the tab in the google sheet workbook to pass to the google sheet function.
                                            The google sheet must be setup with this tab already created.
            null_definitions (list): A list containing elements to be defined as a null value.
            output_method (str): A string identifying whether the output is to be sent to a Google sheet 
                                        ('gsheet') or returned as a dataframe ('Dataframe').
                             
        Returns:
            null_count_df (pandas.DataFrame): Dataframe showing the percentage of nulls in each column grouped 
                                           by the 'cols_to_group'."""
        if null_definitions == None:
            null_definitions = [np.nan,'N/A','','None']

        if cols_to_ignore == None:
            cols_to_ignore = []

        for null in null_definitions: 
            df = df.replace(null, 'NullValue')

        null_count_df = pd.DataFrame(index=df[cols_to_group].value_counts().index).reset_index()
        other_cols = list(set(df.columns) - set(cols_to_group) - set(cols_to_ignore))

        for col in other_cols:
            col_groupby = df.groupby(cols_to_group)[col].apply(lambda x: round(100 * x[x == 'NullValue'].count()/x.shape[0],1)).reset_index()
            null_count_df = pd.merge(null_count_df, col_groupby)

        if output_method == 'gsheet':
            self.util.write_to_gsheet(gsheet_name, tab_name, null_count_df)
        elif output_method == 'Dataframe': 
            return null_count_df
    
    def check_data_recency(
            self, 
            df, 
            cols_to_group,
            gsheet_name, 
            tab_name='DataRecency',
            three_days_for_monday=True,
            date_col='date',
            dayfirst='EnterValue', 
            yearfirst='EnterValue', 
            format=None, 
            errors='raise'
    ):
        """Post a google sheet showing how many days since different channels have been active.
        Also return a list of channels that have been inactive for more than 2 days which
        might be indicative of an error
        
        Args:
            df (pandas.DataFrame): Dataframe of data containing a 'date' column
            cols_to_group (list or str): Columns to groupby effectively creating the 'channels'
            gsheet_name (str): Name of the google sheet to write to
            tab_name (str): Name of the tab in the Google sheet
            three_days_for_monday (bool): If the check is run on a Monday, give 3 days before declaring a channel as inactive because of the weekend.
            date_col (str): Column name for the date to find the maximum value for based on grouping by `cols_to_group`. If 'created' is used when working with Tracer data, then this will find out when the data was last updated by Tracer, regardless of whether the actual date of the post was 30 days ago.
            dayfirst (bool): If True, parses dates with the day first, eg 10/11/12 is parsed as 2012-11-10. If False, parses dates with the month first, eg 10/11/12 is parsed as 2010-11-12. If None, this is set to True if the day is in the first position in the format string, False otherwise. If dayfirst is set to True, parsing will be faster, but will fail for ambiguous dates, such as 01/02/03.
            yearfirst (bool): If True parses dates with the year first, eg 10/11/12 is parsed as 2010-11-12. If both dayfirst and yearfirst are True, yearfirst is preceded (same as dateutil). If False, parses dates with the month first, eg 10/11/12 is parsed as 2012-11-10. If None, this defaults to False. Setting yearfirst to True is not recommended, as it can result in ambiguous dates.
            format (str): Format to use for strptime. If None, the format is inferred from the first non-NaN element of the column. If the format is inferred, it will be used in subsequent parsing, even if the format changes. To specify a format string that will be used in parsing regardless of the inferred format, use pd.to_datetime with format.
            errors (str): If 'raise', then invalid parsing will raise an exception. If 'coerce', then invalid parsing will be set as NaT. If 'ignore', then invalid parsing will return the input.

        Returns:
            error_message (str): String error message describing which channels are recently inactive. This message can then be sent
                                to a slack function"""

        error_message = ''
        organic_or_paid = self.util.identify_paid_or_organic(df)
        print(organic_or_paid)
        buffer_days_since_active = 2
        #remove any timezone information from the date_col and check the date is being read in in correct format
        df[date_col] = pd.to_datetime(df[date_col].dt.date, dayfirst=dayfirst,
                                      yearfirst=yearfirst, format=format, errors=errors)

        data_recency = pd.DataFrame(df.groupby(cols_to_group)[date_col].max().apply(lambda x: (today - x).days)).reset_index().rename(columns={date_col:'DaysSinceActive'})  
    
        self.util.write_to_gsheet(gsheet_name, tab_name, data_recency, sheet_prefix=organic_or_paid)

        #create a tag string to identify a channel, a concatenation of all the column values specified in 'cols_to_group'
        def concat_cols(x):
            result = ''
            for col in cols_to_group:
                result += x[col] + '-'
            return result.rstrip('-')
            
        data_recency['Channel'] = data_recency.apply(lambda x: concat_cols(x),axis=1)

        #The option for giving 3 days of buffer for monday is available in case no advertising/posting is done 
        #over the weekend and you don't want many channels appearing as if they are inactive on Monday morning
        if today.day_of_week == 0 and three_days_for_monday: 
            buffer_days_since_active = 3

        channels_recently_inactive = data_recency.query(f'DaysSinceActive >{buffer_days_since_active} and DaysSinceActive <=6')
        channels_recently_inactive_list = channels_recently_inactive['Channel'].unique().tolist()
        if len(channels_recently_inactive_list) != 0:
            error_message = f'{organic_or_paid} Channels Recently that are recently inactive {" , ".join(channels_recently_inactive_list)}\n'

        return error_message
    
    def boosted_function_qa(
            self, paid_df, 
            organic_df, gsheet_name, 
            tab_name='OrganicWithBigImpressions', 
            impressions_threshold=100000):
        """Takes in organic data and paid data and reports how many paid adverts are mislabelled as boostes and how many organic posts are incorrectly
        said to not have been boosted.
            
        Args:
            paid_df (pandas.DataFrame): Daily ad spend, boosted posts identified in the 'workstream' column
            organic_df (pandas.DataFrame): Organic Post performance Data 
            impressions_threshold (pandas.DataFrame): The number of impressions above which it is unlikely the post is purely organic
            
        Returns:
            error_message (str): A string detailing what the error is so that it can be passed to a notification service like slack"""
        error_message = ''
        organic_df['Error'] = False

        # count number of unique paid posts with workstream organic minus oranic posts labelled as boosted
        # number of boosted post from paid naming convention Tags
        paid_boosted_count = len(paid_df[paid_df['workstream'] == 'boosted']['post_id'].unique())

        # number of posts our boosted matching function has identified as boosted
        organic_boosted_count = len(organic_df[organic_df['workstream'] == 'boosted']['post_id'].unique())
        missing_boosted = paid_boosted_count - organic_boosted_count

        if (missing_boosted) != 0:
            error_message = error_message + '  ' + \
                f'There are {missing_boosted}({round(missing_boosted*100/paid_boosted_count, 2)}%) posts mislabelled as Pure Organic\n'
            logger.warning(error_message)

        # A post that is labelled as organic but has impressions over the impressions_threshold may actually be boosted 
        # but hasn't been identified as such
        misslabelled_og_rows = organic_df[(organic_df['workstream'] == 'Pure Organic') & \
                                        ((organic_df['impressions'] >= impressions_threshold) | \
                                        (organic_df['video_views'] >= impressions_threshold))]

        if len(misslabelled_og_rows.index.values) > 0:
            error_message = error_message + '  ' + \
                f'There are {len(misslabelled_og_rows.index.values)} Pure Organic posts with over {impressions_threshold} impressions or video views\n'
            logger.warning(error_message)
            self.util.write_to_gsheet(gsheet_name, tab_name, misslabelled_og_rows)

        logger.warning(error_message)
        return error_message
    
    def comparison_with_previous_data(
            self, df, name_of_df, 
            cols_to_check=None, perc_increase_threshold=20,
            perc_decrease_threshold=0.5, check_cols_set=True,
            unique_id_cols=None, cols_to_group=None,
            raise_exceptions=True, manual_override=False,
            date_col='date', dayfirst=True, yearfirst=False):
        """ This function allows you to compare the column totals of a dataframe with the totals calculated on a previous time to detect any changes that could be indicative of an error.

            The historic column totals are stored in a datatable for reference and the function will check the current totals with the most recent previous totals and raise an exception 
            if the totals have changed by more than the specified thresholds.

            The function will also check if the columns in the dataframe have changed from the previous time and raise an exception if they have.

            If the previous totals were wrong because of an error and the latest values in the dataframe are correct then you can set manual_override to True and the function will 
            add a new row to the historic db with the new values. This manual override can only be done once in a row to stop someone forgetting they put manual override on and 
            leaving it running in the script which would mean the function would never pick up any errors 

            Args:
                df (pd.DataFrame): Input dataframe that the historic checks are going to be performed on.
                name_of_df (str): Name of the dataframe, this will be used to name a file to save for future comparison.
                cols_to_check (List[str]): A list of strings that detail the columns to be totaled which will then 
                    be compared with previous data.
                perc_increase_threshold (float): The percentage increase threshold above 
                    which it is deemed that the totals have raised too rapidly and an error has occured.
                perc_decrease_threshold (float): The percentage decrease threshold below 
                    which it is deemed that the totals decreased and an error has occured.
                check_cols_set (bool): If true, store the set of columns present for comparison to see if any new 
                    columns have been added or removed next time, in which case it is deemed an error has occured.
                unique_id_cols (List[str]): A list of  the columns that are unique identifiers in order to do a unique ID count to help identify
                    any cause of the change in totals of the cols_to_check.   
                cols_to_group (List[str]): A list the columns that are to be grouped by, and the cols_to_check will be summed for each group. This will be stored as a string in the data table
                    which can be used as a reference to see which groups have caused the change in totals. 
                raise_exceptions (bool): If true, then raise an exception if an error has occured instead of just returning an error message.
                    smanual_override (bool): If true, then add a new row to the historic db with the new values with the new value which is outside the tolerance bounds but is now not considered an error
                date_col (str): The name of the date column in the dataframe
                dayfirst (bool): If True, parses dates with the day first, eg 10/11/12 is parsed as 2012-11-10. If False, parses dates with the month first, eg 10/11/12 is parsed as 2010-11-12. 
                    If None, this is set to True if the day is in the first position in the format string, False otherwise. If dayfirst is set to True, parsing will be faster, but will fail for ambiguous dates, such as 01/02/03.
                yearfirst (bool): If True parses dates with the year first, eg 10/11/12 is parsed as 2010-11-12. If both dayfirst and yearfirst are True, yearfirst is preceded (same as dateutil). 
                    If False, parses dates with the month first, eg 10/11/12 is parsed as 2012-11-10. If None, this defaults to False. Setting yearfirst to True is not recommended, as it can result in ambiguous dates.

            Returns:
                error_message (str): String detailing what the error is so that it can be passed to a notification service like slack.
            
            Raises:
                Exception: If raise_exceptions = True and an error has occured."""
        
        if cols_to_check == None:
            cols_to_check = ['impressions','likes']
        error_message, error_occured = '', False
        client_name = self.util.client_name.lower()
        name_of_table = f'previous_totals_check_{client_name}_{name_of_df}'
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, yearfirst=yearfirst)
        for col in cols_to_check:
            df[col] = pd.to_numeric(df[col],errors='coerce').fillna(0).astype(int)

        # Create a dictionary of the sums of the columns specified in cols_to_check
        new_dict = {}
        new_dict['date'] = str(datetime.now())
        new_dict['min_date'],new_dict['max_date'] = df[date_col].min(), df[date_col].max()
        new_dict['comments'] = ' '

        for col in cols_to_check:
            new_dict[col] = int(df[col].sum())
        if check_cols_set == True:
            new_dict['columns'] = df.columns.tolist()
        if unique_id_cols != None:
            new_dict['num_unique_ids'] = df[unique_id_cols].apply(lambda row: '-'.join(row.values.astype(str)),axis=1).nunique()
        
        # Check if the historic database already exists, if not create it
        if self.util.table_exists(name_of_table):
            historic_db = self.util.read_from_postgresql(name_of_table, clean_date=False)
            historic_db.drop(columns=['DateWrittenToDB'], inplace=True) # This column is not needed for comparison, it gets created when writing to the db
            historic_db = historic_db.reset_index(drop=True)
            historic_db.columns = historic_db.columns.str.lower()

        else:
            historic_db = pd.DataFrame([new_dict])
            self.util.write_to_postgresql(historic_db, name_of_table, if_exists='replace')
            return error_message
        
        #Turn the most recent entry in the historic db into a dict
        old_dict = historic_db.sort_values(by='date', ascending=False).iloc[0].to_dict()

        # for each key in the old dict, check if it is in the new dict and if it is, check if it has increased or decreased too much
        for key, value in old_dict.items():

            if key == 'columns':
                value = value.replace('{', '').replace('}', '')
                value = [part.strip() for part in value.split(',')]
                if set(value) != set(new_dict['columns']):
                    #error_occured = True
                    columns_removed = list(set(value) - set(new_dict['columns']))
                    columns_added = list(set(new_dict['columns']) - set(value))
                    error_message = error_message + '  ' + f'The columns seems to have changed from last time,\n'\
                                            f' Columns that were added = {columns_added}\n' \
                                            f' Columns that were removed = {columns_removed}\n'
            elif key not in cols_to_check: #If the key is not a numerical type column that needs checking then don't run the numerical checks
                continue

            elif new_dict[key] * (1 + perc_decrease_threshold/100) < value:
                error_occured = True
                error_message = error_message + '  ' + f'The total of {key} seems to have decreased from last time\n'\
                                        f' Prev Value = {old_dict[key]} , New Value = {new_dict[key]}\n'
            elif new_dict[key] > value * (1 + perc_increase_threshold/100):
                error_occured = True
                error_message = error_message + '  ' + f'The total of {key} has increased by more than\n'\
                                    f'{perc_increase_threshold}% from last time\n'\
                                        f' Prev Value = {old_dict[key]}, New Value = {new_dict[key]}\n'
                
        # If cols_to_group is specified, then group by those columns and store the info in the dict
        if cols_to_group != None:
            new_dict['info'] = df.groupby(cols_to_group)[cols_to_check].sum().reset_index().to_json(orient='records')
            
        
        #The "Columns" and the "info" columns take up a low of space, so we only keep the last 5 entries in the db
        if check_cols_set == True: 
            cols_to_reduce_data = ['info','columns']
        else:
            cols_to_reduce_data = ['info']

        # If manual override is set to False, then add a new row to the historic db with the new values
        if not manual_override:
            if error_occured:
                error_message = f'Comparison with historic df {name_of_df}: ' + error_message + '\n'
                if cols_to_group != None:
                    error_message = error_message + f"Groupby of latest {name_of_df} = {new_dict['info']}" + '\n'
                logger.info('ERROR' + error_message) # If error messages has been added to then log it
            if raise_exceptions and error_occured:
                if 'num_unique_ids' in old_dict:
                    error_message = error_message + f'\n Unique values count change: {old_dict["num_unique_ids"]} -> {new_dict["num_unique_ids"]}'
                raise Exception(error_message)
            if not error_occured:
                new_dict['manual_override'] = False
                db_with_new_row = pd.concat([historic_db, pd.DataFrame([new_dict])], ignore_index=True)
                db_with_new_row.loc[db_with_new_row.iloc[:-5].index,cols_to_reduce_data] = np.nan
                self.util.write_to_postgresql(db_with_new_row, name_of_table, if_exists='replace')

        else: # If manual override is set to True, then add a new row to the historic db with the new values
            #If the previous entry was not a manual override, then add a new row with the new "correct" value
            if old_dict['manual_override'] == False:
                logger.info('Manual Override set to True, adding new row to historic db with different values now deemed to be correct') 
                new_dict['manual_override'] = True
                db_with_new_row = pd.concat([historic_db, pd.DataFrame([new_dict])], ignore_index=True)
                
                db_with_new_row.loc[db_with_new_row.iloc[:-5].index,cols_to_reduce_data] = np.nan
                self.util.write_to_postgresql(db_with_new_row, name_of_table, if_exists='replace')

            elif old_dict['manual_override'] == True:# If the previous entry was a manual override, don't let multiple overrides happen in a row to stop someone forgetting they put manual override on and leaving it running in the script
                raise Exception('Manual override already in place, not adding another row, please check the new value is'\
                                    'correct and set manual override back to False')
        return error_message

    
    def duplicates_qa(
            self, df, name_of_df, 
            perc_dupes_thresh=3, cols_to_check=None, 
            cols_to_add=None, return_type='duplicates', 
            raise_exceptions=True):
        """Checks for duplicates in a dataframe and returns the duplicates or the dataframe without duplicates.

        The function first checks to see whether the input dataframe is paid or organic data. If it is paid data then the columns to check for duplicates are
        ['date','platform','country','media_type','cohort','message','ad_name','spend'].

        For organic data the standard columns it will check for are ['platform', 'country','media_type' ,'message','url']

        You can specify other columns to check for duplicates by passing a list to the cols_to_check argument. You can also add to the standard columns by passing
        a list to the cols_to_add argument.

        You can specify whether to return the original df, the df with duplicates removed or just the duplicates or nothing by passing 'original', 
        'duplicates' or 'duplicates_removed' or 'nothing' to the return_type argument.

        If the return type is duplicates_removed, an error message will also be returned, with the error message being blank if no duplicates are found. This can be passed
        to a notification function for example.

        
        Args:
            df (pd.DataFrame): The Dataframe to be checked for duplicates
            name_of_df (str): The name of the dataframe to be used for logging purposes
            perc_dupes_thresh (int, optional): The percentage of duplicates that are allowed before an error is raised. Defaults to 3.
            cols_to_check (Optional[list], optional): The columns to check for duplicates. Defaults to None.
            cols_to_add (Optional[list], optional): The columns to add to the standard cols_to_check to duplicates check. Defaults to None.
            return_type (str, optional): The type of return. Either 'duplicates' or 'duplicates_removed'. Defaults to 'duplicates'.
            raise_exceptions (bool, optional): If true raise an exception if duplicates are found. Defaults to True.
        
        Raises:
            Exception: If raise_exceptions = True and duplicates are found
        
        Returns:
            pd.DataFrame: Returns the duplicates,the df without duplicates or the original df depending on the return_type"""

        num_rows = len(df)
        logger.info(f'Num of rows in {name_of_df} = {num_rows}')
        paid_or_organic = self.util.identify_paid_or_organic(df)
        print(f'Paid or Organic? : {paid_or_organic}')
        
        if cols_to_check == None:
            if paid_or_organic == 'Paid':
                #Include date in paid duplicate check because the same ad is repeated across consequitve days
                #Spend is a good indicator of duplicates in paid data
                if 'spend_usd' in df.columns:
                    spend_col = 'spend_usd'
                else:
                    spend_col = 'spend'

                cols_to_check = ['date', 'platform', 'country', 'media_type', 'cohort', 'message', 'ad_name', spend_col]
                
            elif paid_or_organic == 'Organic':
                cols_to_check = ['platform', 'country', 'media_type' , 'message', 'url']
                if 'date_row_added' in df.columns:
                    cols_to_check.append('date_row_added')
        
        if cols_to_add != None:
            cols_to_check = cols_to_check + cols_to_add

        
        for i in range(2, len(cols_to_check)+1):
            num_duplicates = df.duplicated(subset=cols_to_check[:i]).sum()
            logger.info(f'{num_duplicates} - {name_of_df} - {cols_to_check[:i]}')
        
        perc_dupes = df.duplicated(subset=cols_to_check).sum()*100 / num_rows
        exceed_thresh = perc_dupes > perc_dupes_thresh

        logger.info(f'% Dupes of whole subset - {name_of_df} = {perc_dupes}')

        if exceed_thresh:
            error_message = f'% Dupes in {name_of_df} exceeds {perc_dupes_thresh}%'
        else:
            error_message = ''

        if raise_exceptions and exceed_thresh:
            raise Exception(error_message)
        
        elif return_type == 'duplicates': 
            return df[df.duplicated(subset=cols_to_check, keep=False)].sort_values(by=cols_to_check)

        elif return_type == 'duplicates_removed':
            return df.drop_duplicates(subset=cols_to_check, inplace=False), error_message
        
        elif return_type == 'original':
            return df
        
        elif return_type == 'nothing':
            return 

    
    def check_impressions_no_engagements(
            self, df, gsheet_name, 
            tab_name='NoImpressionsButEngagements', 
            raise_exceptions=False):
        """Function to check if a row item has engagements but no impressions and no video views. 
        This shouldn't happen and is indicative of an error with Tracer but can be valid as some 
        platforms count viral engagements differently.

        Args:
            df (DataFrame): Input dataframe of advertising data with columns 'impressions' or 'video_views'
            gsheet_name (str): name of the google sheet
            tab_name (str, optional): name of the tab. Defaults to 'NoImpressionsButEngagements'.
            raise_exceptions (bool, optional): Boolean flag if set to true will raise an exception if an offending row item is discovered. Defaults to False.

        Returns:
            error_message (str): String detailing what the error is so that it can be passed to a notification service like slack."""

        paid_or_organic = self.util.identify_paid_or_organic(df)
        # https://docs.google.com/spreadsheets/d/1refbPLje6B48qvSRNXrK3U_OAfzjzeuTrzgfqq9O-yw/edit#gid=0
        df['date'] = pd.to_datetime(df['date'])
        error_message = f'{paid_or_organic} Data Quality Check Function Failed'
        engagements_cols = ['likes', 'comments', 'shares']
        engagements_mask = ((df[engagements_cols] != 0).any(1))
        # Check for zeros in impression columns but non zeros in likes, impressions, and other metrics
        no_impr_but_engage = list(df[(df['impressions'] == 0) & engagements_mask].index.values)

        # TikTok organic doesn't have impressions, instead it has video views. There is an error if
        # Video views is equal to zero but there are engagements
        tiktok_org_no_impr_but_engage = list(df[((df['platform'] == 'TikTok') & (df['workstream'] == 'boosted') &
                                                (df['video_views'] == 0) & engagements_mask)].index.values)

        reels_org_no_impr_but_engage = list(df[((df['placement'] == 'Reels') & (df['workstream'] == 'boosted') &
                                                (df['video_views'] == 0) & engagements_mask)].index.values)

        # paid with  impressions but has engagements
        no_impressions_but_engage_rows = set(no_impr_but_engage + tiktok_org_no_impr_but_engage 
                                            + reels_org_no_impr_but_engage)

        if len(no_impressions_but_engage_rows) > 0:
            error_message = error_message + '  ' + f'There are {len(no_impressions_but_engage_rows)} rows with'\
                ' zeros in impressions columns but non-zeros in likes, comments, shares and video_views\n'

            df.loc[no_impressions_but_engage_rows, 'Error'] = True
            # error_message = error_message + " " + "Split By platform"
            # error_message = error_message + " " + pd.crosstab(erroneous_df['platform'],erroneous_df['media_type'],margins=True,dropna=False)
            self.util.write_to_gsheet(gsheet_name, tab_name, df.loc[no_impressions_but_engage_rows],
                            sheet_prefix=paid_or_organic)
            exception_occurred = True

        logger.warning(error_message)
        if raise_exceptions and exception_occurred:
            raise Exception(error_message)
        
        return error_message

    def naming_convention_checker(
            self, df, gsheet_name, 
            naming_convention, 
            campaignname_dict=None, adgroupname_dict=None, 
            adname_dict=None, campaign_col='campaign_name', 
            adgroup_col='group_name', adname_col='ad_name',
            spend_col='spend_usd', start_char='_', 
            middle_char=':', end_char='_',
            platform_col='platform', 
            check_meta_platform=True):
        """Checks for naming convention errors in a given DataFrame and outputs the errors to a Google Sheet.

        The function takes in a DataFrame containing paid data with columns for campaign name, ad group name, and ad name, 
        as well as a DataFrame containing the accepted tags and values for each level of naming (campaign, ad group, ad name). 
        Dictionarys for each level fo checking are required this specifies what tags should be checked for, e.g. should a check
        for the correct values in 'platform' be performed. If a dictionary for a certain level is not provided that level will
        not be checked. The function will output a table in a Google Sheet showing all errors in naming conventions, 
        with a tab for each level of errors.

        The Google Sheet should be set up with a tab name for each level of errors to check for, 
        i.e. 'CampaignNameErrors', 'AdGroupNameErrors', 'AdNameErrors'.

        Args:
            df (DataFrame): The DataFrame containing paid data with campaign name, ad group name, and ad name columns.
            naming_convention (DataFrame): The DataFrame containing the accepted tags and values for each level of naming.
            campaignname_dict (dict, optional): A dictionary of the tags to be checked for the campaign name, with the column label as the key and the shortcode as the value.
            adgroupname_dict (dict, optional): A dictionary of the tags to be checked for the ad group name, with the column label as the key and the shortcode as the value.
            adname_dict (dict, optional): A dictionary of the tags to be checked for the ad name, with the column label as the key and the shortcode as the value.
            campaign_col (str, optional): The column in the input DataFrame that corresponds to the campaign name. Defaults to 'campaign_name'.
            adgroup_col (str, optional): The column in the input DataFrame that corresponds to the ad group name. Defaults to 'group_name'.
            adname_col (str, optional): The column in the input DataFrame that corresponds to the ad name. Defaults to 'name'.
            start_char (str, optional): The starting character for the tag in the naming convention. Defaults to '_'.
            middle_char (str, optional): The character that separates the tag from the value in the naming convention. Defaults to ':'.
            end_char (str, optional): The ending character for the tag in the naming convention. Defaults to '_'.

        Returns:
            Writes to a Google Sheet a table with the index being a unique instance of the campaign."""
        
        conv_level_tuple = ((campaignname_dict, campaign_col, 'CampaignNameErrors'), (adgroupname_dict, adgroup_col, 'AdGroupNameErrors'),
                            (adname_dict, adname_col, 'AdNameErrors'))
        # df = clean.clean_column_names(df)
        #Obtain the column names of all the Keys available in the convention tracker sheet
        #https://docs.google.com/spreadsheets/d/1Wtwd6xT9zRLhPICWSWDNZ2mXBy74_xszLVX2ZqO_odo/edit#gid=605935420
        key_values_conv_cols = [x.replace(' Key', '') for x in naming_convention.columns if ' Key' in x]

        def remove_empties_from_list(input_list):
            '''Obtaining a list of keys from the tracker sheet column often results in multiple empty
                strings, this function removes them from the list'''
            return list(filter(lambda x: x != '', input_list))

        def return_value(string, tag, acceptable_values=None):
            """This checks for each campaign, group_name or ad_name string, for a certain tag e.g.'pl'
            whether the tag is even present and if so whether a correct value is present"""
            search_string = f'{start_char}{tag}{middle_char}(.*?){end_char}'
            search_result = re.search(search_string, string + '_') #add a underscore at so search string has an endpoint to find
            if search_result == None:
                return 'NoKey'
            elif search_result.group(1).strip(' ') == '':
                return 'NoKey'
            else:
                if acceptable_values == None:
                    return "CorrectKeyPresent"
                if search_result.group(1).upper() in acceptable_values:
                    return "Correct"
                else:
                    return 'Incorrect Value'
        
        for i,level in enumerate(conv_level_tuple):
            checking_cols = []
            if 'video_views' in df.columns:
                cols_to_sum = [spend_col, 'video_views']
            else:
                cols_to_sum = spend_col
            
            if i == 2:
                agg_dict = {col: 'sum' for col in cols_to_sum}
                agg_dict[platform_col] = 'first'
                output_df = round(df.groupby(level[1]).agg(agg_dict).reset_index(),1)

                if check_meta_platform:
                    checking_cols.append('platform_meta_check')
                    def check_meta_platform_function(row):
                        if row[platform_col] == 'Facebook':
                            if f'{middle_char}IG{end_char}' in row[adname_col]:
                                return 'IG in ad name but platform is Facebook'
                        elif row[platform_col] == 'Instagram':
                            if f'{middle_char}FB{end_char}' in row[adname_col]:
                                return 'FB in ad name but platform is Instagram'
                    
                    output_df['platform_meta_check'] = output_df.apply(lambda x: check_meta_platform_function(x), axis=1)
            #For each level of the naming convention, i.e. campaign, adgroup, adname
            else:
                output_df = round(df.groupby(level[1])[cols_to_sum].sum().reset_index(),1)

            if level[0] == None: continue #no convention dict provided therefore ignore
            for label,tag in level[0].items():
                #For each label and tag in the required set 
                if label in key_values_conv_cols:
                    acceptable_values= remove_empties_from_list(naming_convention[label+' Key'].str.upper().unique().tolist())
                    output_df[f'{label} ("{tag}")'] = output_df[level[1]].apply(lambda x: return_value(x, tag, acceptable_values))
                    checking_cols.append(f'{label} ("{tag}")')
                else:
                    output_df[f'{label} ("{tag}")'] = output_df[level[1]].apply(lambda x: return_value(x, tag))
                    checking_cols.append(f'{label} ("{tag}")')
                
            output_df = output_df.sort_values(by=checking_cols, ascending=False)
            
                
            self.util.write_to_gsheet(workbook_name = gsheet_name, sheet_name= level[2], df = output_df)
    
