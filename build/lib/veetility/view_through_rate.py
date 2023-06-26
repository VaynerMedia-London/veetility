from . import cleaning_functions as clean
from . import utility_functions
import pandas as pd
import numpy as np
import pickle
import os

class VTRConversion:

    def __init__(self):
        pass

    def create_seconds_vtr_metrics(self,ads_df):
        ads_df = self.calc_vtr_rates(ads_df)
        ads_df = self.convert_video_len_to_seconds(ads_df)
        #self.ads_df = ads_df
        return ads_df

    def create_grouped_vtr_metrics(self,ads_df, groupby_cols):
        # ads_df = self.create_seconds_vtr_metrics(ads_df)
        ads_df_grouped = ads_df.groupby(groupby_cols).apply(self.group_by_asset).reset_index()
        #self.ads_df_grouped = ads_df_grouped
        return ads_df_grouped


    def convert_video_len_to_seconds(self, df,video_len_col='video_length__tags'):
        df['video_length'] = df.apply(lambda x: clean.video_len_toseconds(x[video_len_col]), axis=1)
        df = df[(df['video_length']!='n/a')&(df['video_length']!='None')&(df['video_length']!='Undefined')]
        return df

    def calc_vtr_rates(self,df):
        if 'likes' in df.columns:
            df['engagements'] = df['likes'] + df['comments'] + df['shares']
            df['engagement_rate'] = round(df['engagements'] * 100 / df['impressions'], 2)
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
    
    
    def group_by_asset(self,x):
        """Create metrics at the asset level, averaging the %VTR metrics across all days the Ad is run
        
        Args:
            x (pd.DataFrame): DataFrame of Ads grouped by asset_id
        
        Returns:
            pd.DataFrame: DataFrame of Ads grouped by asset_id with metrics at the asset level such as 
            average %VTR, average engagement rate, and coordinates of points for the VTR curve """
        d = {}
        coordinates_list = []
        if 'message' in x.columns:
            d['message'] = x['message'].iloc[0]
        d['num_days_ran'] = x.shape[0]
        d['impressions'] = x['impressions'].sum()
        
        if 'engagements' in x.columns:
            d['engagements'] = x['engagements'].sum()
            d['engagement_rate'] = round(d['engagements'] * 100 / d['impressions'], 2)
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
        d['video_length'] = x['video_length'].iloc[0]
        perc_25_in_secs = round(x['video_length'].iloc[0] * 0.25, 2)
        perc_50_in_secs = round(x['video_length'].iloc[0] * 0.5, 2)
        perc_75_in_secs = round(x['video_length'].iloc[0] * 0.75, 2)

        coordinates_list.append([(perc_25_in_secs, d['25%VTR']), (perc_50_in_secs, d['50%VTR']), 
                                (perc_75_in_secs, d['75%VTR']), (d['video_length'], d['100%VTR'])])
        
        coordinates = np.array([coordinates_list])
        points_indices = np.argsort(coordinates[:,0])
        d['coordinates'] = coordinates[points_indices]

        return pd.Series(d, index=list(d.keys()))


    def run_ml_on_each_video(self,row):
        """Run a linear regression on the VTR data for each video.
        
        Args:
            row (pandas.Series): A pandas Series containing the VTR data for a single video.
        
        Returns:
            pandas.Series: A pandas Series containing the VTR data for a single video, with the ML model predictions."""
        
        seconds_milestone_array = np.array(row['coordinates'])[:,0]
        percentage_reached_array = np.array(row['coordinates'])[:,1]
        
        point_to_point_linear_model = utility_functions.PointToPointRegressor()
        point_to_point_linear_model.fit(seconds_milestone_array, percentage_reached_array)
        d = {}
        video_length = row['video_length']
        d['video_length'] = video_length
        #point_to_point_linear_model.plot()
        #Set default values
        d['5secVTR%'] = np.nan
        d['10secVTR%'] = np.nan
        d['15secVTR%'] = np.nan
        d['20secVTR%'] = np.nan
        d['25secVTR%'] = np.nan
        d['30secVTR%'] = np.nan
        if video_length >= 5:
            d['5secVTR%'] = round(point_to_point_linear_model.predict(5),2)
        if video_length >= 10:
            d['10secVTR%'] = round(point_to_point_linear_model.predict(10),2)
        if video_length >= 15:
            d['15secVTR%'] = round(point_to_point_linear_model.predict(15),2)
        if video_length >= 20:
            d['20secVTR%'] = round(point_to_point_linear_model.predict(20),2)
        if video_length >= 25:
            d['25secVTR%'] = round(point_to_point_linear_model.predict(25),2)
        if video_length >= 30:
            d['30secVTR%'] = round(point_to_point_linear_model.predict(30),2)

        return pd.Series(d,index=list(d.keys()))
    