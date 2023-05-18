import pandas as pd
import regex as re
import numpy as np
from datetime import date,datetime
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

    def __init__(self,util_object=None):

        if util_object != None:
            self.util = util_object
    
    def null_values_checker(self,df,cols_to_group,cols_to_ignore,gsheet_name,tab_name,
                    null_definitions=[np.nan,'N/A','','None'],output_method='gsheet'):
        """Takes in a dataframe and columns to groupby and checks how many null values or null equivalents
        there are in the rest of the columns.
            
        Args:
            df (pandas.DataFrame): The Input Dataframe of any type where you want to check for nulls.
            cols_to_group (list of str): The list of columns to perform a groupby operation with the null 
                                        percentage counts will be a percentage of nulls in these groupbys.
            cols_to_ignore (list of str): The list of columns to not count nulls in, to make the output 
                                        dataframe smaller perhaps.
            gsheet_name (str): The name of the google sheet workbook to pass to the google sheet function.
            tab_name (str): The name of the tab in the google sheet workbook to pass to the google sheet function.
                                            The google sheet must be setup with this tab already created.
            null_definitions (list): A list containing elements to be defined as a null value.
            output_method (str): A string identifying whether the output is to be sent to a Google sheet 
                                        ('gsheet') or returned as a dataframe ('Dataframe').
                             
        Returns:
            null_count_df (pandas.DataFrame): Dataframe showing the percentage of nulls in each column grouped 
                                           by the 'cols_to_group'."""

        for null in null_definitions: 
            df = df.replace(null,'NullValue')

        null_count_df = pd.DataFrame(index=df[cols_to_group].value_counts().index).reset_index()
        other_cols = list(set(df.columns) - set(cols_to_group) - set(cols_to_ignore))

        for col in other_cols:
            col_groupby = df.groupby(cols_to_group)[col].apply(lambda x: round(100 * x[x == 'NullValue'].count()/x.shape[0],1)).reset_index()
            null_count_df = pd.merge(null_count_df,col_groupby)

        if output_method == 'gsheet':
            self.util.write_to_gsheet(gsheet_name,tab_name,null_count_df)
        elif output_method == 'Dataframe': 
            return null_count_df
    
    def check_data_recency(self,df,cols_to_group,gsheet_name,tab_name='DataRecency',
                            three_days_for_monday= True,date_col='date',
                            dayfirst='EnterValue',yearfirst='EnterValue',format=None,errors='raise'):
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
        df[date_col] = pd.to_datetime(df[date_col].dt.date,dayfirst=dayfirst,
                                      yearfirst=yearfirst,format=format,errors=errors)

        data_recency = pd.DataFrame(df.groupby(cols_to_group)[date_col].max().apply(lambda x: (today - x).days)).reset_index().rename(columns={date_col:'DaysSinceActive'})  
    
        self.util.write_to_gsheet(gsheet_name,tab_name,data_recency,sheet_prefix=organic_or_paid)

        #create a tag string to identify a channel, a concatenation of all the column values specified in 'cols_to_group'
        def concat_cols(x):
            result=''
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
    
    def boosted_function_qa(self,paid_df, organic_df, gsheet_name,tab_name='OrganicWithBigImpressions',impressions_threshold = 100000):
        """Takes in organic data and paid data and reports how many are mislabelled as boosted
            
        Args:
            paid_df (pandas.DataFrame): Daily ad spend, boosted posts identified in the 'workstream' column
            organic_df (pandas.DataFrame): Organic Post performance Data 
            impressions_threshold (pandas.DataFrame): The number of impressions above which it is unlikely the post is purely organic
            
        Returns:
            error_message (str): A string detailing what the error is so that it can be passed to a notification service like slack"""
        error_message = ''
        organic_df['Error'] = False

        # count number of unique paid posts wi  th workstream organic minus oranic posts labelled as boosted
        # number of boosted post from paid naming convention Tags
        paid_boosted_count = len(paid_df[paid_df['workstream'] == 'boosted']['post_id'].unique())

        # number of posts our boosted matching function has identified as boosted
        organic_boosted_count = len(organic_df[organic_df['workstream'] == 'boosted']['post_id'].unique())
        missing_boosted = paid_boosted_count-organic_boosted_count

        if (missing_boosted) != 0:
            error_message = error_message + '  ' + \
                f'There are {missing_boosted}({round(missing_boosted*100/paid_boosted_count,2)}%) posts mislabelled as Pure Organic\n'
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
            self.util.write_to_gsheet(gsheet_name,tab_name, misslabelled_og_rows)
        # return TT or IG values with empty message field
        # for the date function exlude the dates we paused for the queen

        logger.warning(error_message)
        return error_message
    
    def comparison_with_previous_data(self,df,name_of_df,cols_to_check=['impressions','likes'],perc_increase_threshold=20,
                                   perc_decrease_threshold=0.5, check_cols_set=True,raise_exceptions=False):
        """ This function stores the high level sums for a datatable from the previous run of the script
        and if they have reduced or increased too sharply an error is raised.
        Args:
            df (pd.DataFrame): Input dataframe that the historic checks are going to be performed on.
            name_of_df (str): Name of the dataframe, this will be used to name a file to save for future comparison.
            cols_to_check (List[str]): A list of strings that detail the columns to be totaled which will then 
                be compared with previous data.
            perc_increase_threshold (float): A number between 0 and 100, the percentage increase threshold above 
                which it is deemed that the totals have raised too rapidly and an error has occured.
            perc_decrease_threshold (float): A number between 0 and 100, the percentage decrease threshold below 
                which it is deemed that the totals decreased and an error has occured.
            check_cols_set (bool): If true, store the set of columns present for comparison to see if any new 
                columns have been added or removed next time, in which case it is deemed an error has occured.
            raise_exceptions (bool): If true, then raise an exception if an error has occured instead of just returning an error message.

        Returns:
            error_message (str): String detailing what the error is so that it can be passed to a notification service like slack."""
        
        error_message, error_occured = '', False
        new_dict = {}
        new_dict['datetime'] = str(datetime.now())
        for col in cols_to_check:
            new_dict[col] = int(df[col].sum())

        if check_cols_set == True:
            new_dict['Columns'] = df.columns.tolist()

        if os.path.isdir('Historic df Comparison (Do Not Delete)') == False:
            os.mkdir('Historic df Comparison (Do Not Delete)')
        if os.path.exists(f'Historic df Comparison (Do Not Delete)/{name_of_df}_previous_totals.json') ==False:
            self.util.write_json(new_dict,f'{name_of_df}_previous_totals',file_type='append',folder='Historic df Comparison (Do Not Delete)/')
            logger.info(f"Creation of {name_of_df}_previous_totals")
            logger.info(new_dict)
            return
        else:
            #old_dict = self.util.unpickle_data(f'{name_of_df}_previous_totals',folder='Historic df Comparison (Do Not Delete)')
            old_dict_file = self.util.read_json(f'{name_of_df}_previous_totals',folder='Historic df Comparison (Do Not Delete)',
                                                    file_type='append')
            old_dict = old_dict_file[-1] #Grab the lastest entry in the json file, descending date order
        
        for key, value in old_dict.items():
            if key == 'Columns':
                if set(value) != set(new_dict['Columns']):
                    error_occured = True
                    columns_removed = list(set(value) - set(new_dict['Columns']))
                    columns_added = list(set(new_dict['Columns']) - set(value))
                    error_message = error_message + '  ' + f'The columns seems to have changed from last time,\n'\
                                            f' Columns that were added = {columns_added}\n' \
                                            f' Columns that were removed = {columns_removed}\n'
            elif key == 'datetime':
                continue
            elif new_dict[key] *(1+perc_decrease_threshold/100) < value:
                error_occured = True
                error_message = error_message + '  ' + f'The total of {key} seems to have decreased from last time\n'\
                                        f' Prev Value = {old_dict[key]} , New Value = {new_dict[key]}\n'
            elif new_dict[key] > value*(1 +perc_increase_threshold/100):
                error_occured = True
                error_message = error_message + '  ' +f'The total of {key} has increased by more than\n'\
                                    f'{perc_increase_threshold}% from last time\n'\
                                        f' Prev Value = {old_dict[key]} , New Value = {new_dict[key]}\n'

        if error_occured:
            error_message = f'Comparison with historic df {name_of_df}: ' + error_message +'\n'
            logger.info('ERROR' + error_message) #if error messages has been added to then log it
        if raise_exceptions and error_occured:
            raise Exception(error_message)
        #self.util.pickle_data(new_dict,f'{name_of_df}_previous_totals',folder='Historic df Comparison (Do Not Delete)/')
        self.util.write_json(new_dict,f'{name_of_df}_previous_totals',file_type='append',
                             folder='Historic df Comparison (Do Not Delete)/')
        
        return error_message
    
    def duplicates_qa(self, df: pd.DataFrame, df_name: str,subset= None, drop_duplicates: bool = True):
        """Checks for duplicates and optionally drops duplicates in a dataframe.
        
        Args:
            df (pd.DataFrame): The Dataframe to be checked for duplicates
            df_name (str): The name of the dataframe to be used for logging purposes
            subset (Optional[Union[list, str]], optional): Only consider certain columns for identifying duplicates, by default use all of the columns
            drop_duplicates (bool, optional): If true remove duplicate values
            
        Returns:
            df (pd.DataFrame): Returns original dataframe without duplicates if 'drop_duplicates' = True"""

        num_duplicates = df.duplicated(subset=subset).sum()
        if num_duplicates >= 0:
            logger.warning(f'{num_duplicates} duplicates found in the {df_name}. Subset = {subset}')
            if drop_duplicates:
                df.drop_duplicates(subset=subset, inplace=True)
        return df

    
    def check_impressions_no_engagements(self,df,gsheet_name,tab_name='NoImpressionsButEngagements',raise_exceptions=False):
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

    def naming_convention_checker(self, df,gsheet_name,naming_convention,campaignname_dict=None, adgroupname_dict=None, 
                                    adname_dict=None,campaign_col='campaign_name',adgroup_col='group_name',adname_col='name',
                                    start_char='_',middle_char=':',end_char='_'):
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
        
        conv_level_tuple = ((campaignname_dict,campaign_col,'CampaignNameErrors'),(adgroupname_dict,adgroup_col,'AdGroupNameErrors'),
                            (adname_dict,adname_col,'AdNameErrors'))
        # df = clean.clean_column_names(df)
        #Obtain the column names of all the Keys available in the convention tracker sheet
        #https://docs.google.com/spreadsheets/d/1Wtwd6xT9zRLhPICWSWDNZ2mXBy74_xszLVX2ZqO_odo/edit#gid=605935420
        key_values_conv_cols = [x.replace(' Key','') for x in naming_convention.columns if ' Key' in x]

        def remove_empties_from_list(input_list):
            '''Obtaining a list of keys from the tracker sheet column often results in multiple empty
                strings, this function removes them from the list'''
            return list(filter(lambda x: x != '',input_list))

        def return_value(string,tag,acceptable_values):
            """This checks for each campaign, group_name or ad_name string, for a certain tag e.g.'pl'
            whether the tag is even present and if so whether a correct value is present"""
            search_string = f'{start_char}{tag}{middle_char}(.*?){end_char}'
            search_result = re.search(search_string, string + '_') #add a underscore at so search string has an endpoint to find
            if search_result == None:
                return 'NoKey'
            elif search_result.group(1).strip(' ') == '':
                return 'NoKey'
            else:
                if search_result.group(1).upper() in acceptable_values:
                    return "Correct"
                else:
                    return 'Incorrect Value'

        for level in conv_level_tuple:
            #For each level of the naming convention, i.e. campaign, adgroup, adname
            output_df = round(df.groupby(level[1])['spend'].sum().reset_index(),1)
            if level[0] == None: continue #no convention dict provided therefore ignore
            for label,tag in level[0].items():
                #For each label and tag in the required set 
                if label in key_values_conv_cols:
                    acceptable_values= remove_empties_from_list(naming_convention[label+' Key'].str.upper().unique().tolist())
                    output_df[f'{label} ("{tag}")'] = output_df[level[1]].apply(lambda x: return_value(x,tag,acceptable_values))
            self.util.write_to_gsheet(workbook_name = gsheet_name,sheet_name= level[2],df = output_df)
    
