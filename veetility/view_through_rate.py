import point_to_point_regressor as ptp
import pandas as pd
import numpy as np
import pickle
import os
from . import cleaning_functions as clean

class ViewThroughRateAnalysis:
    """Class for analysing View Through Rate (VTR) metrics of Video Ads from Social Media platforms such as TikTok and Meta (Facebook and Instagram).

    The platforms provide the VTR metrics in terms of the number of people who viewed 25%, 50%, 75% and 100% of the video. This is inconvenient
    because the videos are at greatly varying lengths. This class converts the percentages to seconds of the video watched.

    Once the VTR% at different points of time is calculated we can then fit a point to point regression model to the data to predict the VTR% at any point in time.
    This is used to create decay curves and also %VTR rate at a defined time (e.g. 15 seconds) consistent across videos of different lengths. 
    
    This 15 second VTR rate can then be used as an engagement metric which we have shown in studies is not correlated to standard engagement metrics such
    as likes per impressions and therefore provides a completely different perspective on the performance of the Ad. It appears a large proportion of people
    watch the video for a large amount of time but just don't like or comment etc on the video. This captures the "hidden engagement" of the video.
    
    This 15second VTR rate has also been shown to not be correlated to video length and therefore can be used to compare videos of different lengths."""

    def __init__(self):
        pass

    def convert_video_len_to_seconds(self, df,video_len_col='video_length__tags'):
        """Convert video length in format from Tracer usually "1:20" to seconds (80 seconds in this case)

        Args:
            df (pd.DataFrame): DataFrame of Ads with a column of video lengths (named video_len_col) in the format "1:20"

        Returns:
            pd.DataFrame: DataFrame of Ads with a column of video lengths in seconds (named video_len_col) in the format 80"""
        
        df['video_length'] = df.apply(lambda x: clean.video_len_toseconds(x[video_len_col]), axis=1)
        df = df[(df['video_length']!='n/a')&(df['video_length']!='None')&(df['video_length']!='Undefined')]
        return df


    def calc_vtr_rates(self,df):
        """Calculate VTR metrics for each Ad by converting the absolute values of the metrics to percentages
        
        The Social media platforms such as TikTok and Meta report the VTR metrics as absolute values.I.e. 324 people watched 25% of the video.
        This function converts the absolute values to percentages. e.g. 324 people watched 25% of the video out of 1000 impressions, therefore
        the 25%VTR is 32.4%. 
        The platform also sometimes provide the absolute number of people who made it to 2,3 or 6 seconds into the video. This function converts
        those absolute values to percentages as well.
        
        Args:
            df (pd.DataFrame): DataFrame of Ads
        
        Returns:
            pd.DataFrame: DataFrame of Ads with VTR metrics as percentages as extra columns"""
        
        if 'likes' in df.columns:
            df['engagements'] = df['likes'] + df['comments'] + df['shares']
            df['engagement_rate'] = round(df['engagements'] * 100 / df['impressions'], 2)
        if 'video_plays' in df.columns:
            df['0s%VTR'] = round(df['video_plays'] * 100 / df['impressions'], 2)
        if 'ad_video_watched_2_s' in df.columns:
            df['2s%VTR'] = round(df['ad_video_watched_2_s'] * 100 / df['impressions'], 2)
        if 'ad_video_watched_3_s' in df.columns:
            df['3s%VTR'] = round(df['ad_video_watched_3_s'] * 100 / df['impressions'], 2)
        if '6s%VTR' in df.columns:
            df['6s%VTR'] = round(df['ad_video_watched_6_s'] * 100 / df['impressions'], 2)
        df['25%VTR'] = round(df['ad_video_views_p_25'] * 100 / df['impressions'], 2)
        df['50%VTR'] = round(df['ad_video_views_p_50'] * 100 / df['impressions'], 2)
        df['75%VTR'] = round(df['ad_video_views_p_75'] * 100 / df['impressions'], 2)
        df['100%VTR'] = round(df['video_completions'] * 100 / df['impressions'], 2)
        return df
    
    
    def group_by_vtr_calcs(self,x, cols_to_keep=None,ad_id_col=None):
        """Create metrics at the asset level, averaging the %VTR metrics across all of the rows for each asset_id

        Social media platforms such as TikTok and Meta provide the VTR metrics at the ad level but with each day existing on a separate row.
        This function groups the rows by asset_id and calculates the average %VTR metrics for each asset_id. It also calculates the average
        engagement rate for each asset_id.

        This function also creates a coordinates column for each asset_id. The coordinates column is a list of tuples of the form (x,y) where
        x is the number of seconds into the video and y is the %VTR at that point in time. This can be used to create the VTR curve for each asset_id.
        
        Args:
            x (pd.DataFrame): DataFrame of Ads grouped by asset_id
        
        Returns:
            pd.DataFrame: DataFrame of Ads grouped by asset_id with metrics at the asset level such as 
                average %VTR, average engagement rate, and coordinates of points for the VTR curve """
        
        d = {}
        coordinates_list = []

        if cols_to_keep is not None:
            for col in cols_to_keep:
                d[col] = x[col].iloc[0]

        d['num_rows'] = x.shape[0]
        if ad_id_col is not None:
            d['num_ad_ids'] = x[ad_id_col].nunique()
        d['impressions'] = x['impressions'].sum()
        
        if 'engagements' in x.columns:
            d['engagements'] = x['engagements'].sum()
            d['engagement_rate'] = round(d['engagements'] * 100 / d['impressions'], 2)
        if '0s%VTR' in x.columns:
            d['0s%VTR'] = round(x['0s%VTR'].mean(), 2)
            coordinates_list.append((0, d['0s%VTR']))
        if '2s%VTR' in x.columns:
            d['2s%VTR'] = round(x['2s%VTR'].mean(), 2)
            coordinates_list.append((2, d['2s%VTR']))
        if '3s%VTR' in x.columns:
            d['3s%VTR'] = round(x['3s%VTR'].mean(), 2)
            coordinates_list.append((3, d['3s%VTR']))
        if '6s%VTR' in x.columns:
            d['6s%VTR'] = round(x['6s%VTR'].mean(), 2)
            coordinates_list.append((6, d['6s%VTR']))

        d['25%VTR'] = round(x['25%VTR'].mean(), 2)
        d['50%VTR'] = round(x['50%VTR'].mean(), 2)
        d['75%VTR'] = round(x['75%VTR'].mean(), 2)
        d['100%VTR'] = round(x['100%VTR'].mean(), 2)
        d['50%VTR_std'] = round(x['50%VTR'].std(), 2)
        d['video_length'] = x['video_length'].max()
        perc_25_in_secs = round(d['video_length'] * 0.25, 2)
        perc_50_in_secs = round(d['video_length'] * 0.5, 2)
        perc_75_in_secs = round(d['video_length'] * 0.75, 2)

        coordinates_list.extend([(perc_25_in_secs, d['25%VTR']), (perc_50_in_secs, d['50%VTR']), 
                                (perc_75_in_secs, d['75%VTR']), (d['video_length'], d['100%VTR'])])
        
        coordinates = np.array(coordinates_list)

        points_indices = np.argsort(coordinates[:,0])
        d['coordinates'] = coordinates[points_indices]

        return pd.Series(d, index=list(d.keys()))
    
    def run_ml_on_each_creative(self, x, seconds_list=None):
        """Run a point-to-point linear regression on each asset_id to predict the %VTR at multiple points in time

        Given we have the coordinates of the VTR curve for each asset_id, for example %View through rate at 3s, 6s, 9s,
        this function will predict the %VTR at the seconds specified in "seconds_list" using a point-to-point linear regression model

        A dataframe will be returned with a coordinates column for each asset_id. The coordinates column is a list of tuples of the form (x,y) where
        x is the number of seconds into the video and y is the %VTR at that point in time. This can be used to create the VTR curve for each asset_id.

        Args:
            x (pd.DataFrame): DataFrame of Ads grouped by asset_id
            seconds_list (list): List of seconds into the video to predict the %VTR at. Defaults to [5,10,15,20,25,30]
        
        Returns:
            pd.DataFrame: DataFrame of Ads grouped by asset_id with a coordinates column for each asset_id. The coordinates column is a list of tuples of the form (x,y) where"""
        coordinates_list = []
        if seconds_list is None:
            seconds_list = [5,10,15,20,25,30]
        try:
            seconds_milestone_array = np.array(x['coordinates'])[:,0]
            percentage_reached_array = np.array(x['coordinates'])[:,1]

            point_to_point_linear_model = ptp.PointToPointRegressor()
            point_to_point_linear_model.fit(seconds_milestone_array, percentage_reached_array)
            d = {}
            video_length = x['video_length']
            d['video_length'] = video_length
            
            for seconds in seconds_list:
                d[f'{seconds}secVTR%'] = np.nan
                if video_length >= seconds:
                    d[f'{seconds}secVTR%'] = round(point_to_point_linear_model.predict(seconds),2)
                    coordinates_list.append([seconds,round(point_to_point_linear_model.predict(seconds),2)])
            
            coordinates = np.array(coordinates_list)
            points_indices = np.argsort(coordinates[:,0])
            d['coordinates'] = coordinates[points_indices]
            return pd.Series(d,index=list(d.keys()))

        except:
            pass
    
    def group_assets_secs_again(self,x, cols_to_keep=None,ad_id_col=None,video_length_col='video_length'):
        """This function groups the data again after first grouping by asset, for example if you want to group all assets from a certain 
        country or campaign together and calculate the average VTR metrics for that country. 
        
        Args:
            x (pd.DataFrame): DataFrame of Ads grouped by asset_id
        
        Returns:
            pd.DataFrame: DataFrame of Ads grouped by asset_id with metrics at the asset level such as 
            average %VTR, average engagement rate, and coordinates of points for the VTR curve """
        d = {}
        coordinates_list = []

        if cols_to_keep is not None:
            for col in cols_to_keep:
                d[col] = x[col].iloc[0]

        d['num_rows'] = x.shape[0]
        if ad_id_col is not None:
            d['num_ad_ids'] = x[ad_id_col].nunique()

        if video_length_col in x.columns:
            d['avg_video_length'] = x[video_length_col].mean()

        d['impressions'] = x['impressions'].sum()

        secs_vtr_cols = [col for col in x.columns if 'secVTR%' in col]
        
        for col in secs_vtr_cols:
            d[col] = round(x[col].mean(), 2)
            coordinates_list.append((int(col.split('sec')[0]), d[col]))
    
        
        coordinates = np.array(coordinates_list)
        points_indices = np.argsort(coordinates[:,0])
        d['coordinates'] = coordinates[points_indices]

        return pd.Series(d, index=list(d.keys()))
    