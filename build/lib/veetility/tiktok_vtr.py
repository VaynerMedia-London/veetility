class VTRConversion:

    def __init__():
        pass

    def create_seconds_vtr_metrics(self,ads_df):
        ads_df = self.calc_vtr_rates(ads_df)
        ads_df = self.convert_video_len_to_seconds(ads_df)
        return ads_df

    def create_grouped_vtr_metrics(self,ads_df, groupby_cols):
        # ads_df = self.create_seconds_vtr_metrics(ads_df)
        ads_df_grouped = ads_df.groupby(groupby_cols).apply(self.tiktok_group_by_asset).reset_index()
        return ads_df_grouped


    def convert_video_len_to_seconds(self, df):
        df['video_length'] = df.apply(lambda x: clean.video_len_toseconds(x['video_length__tags']), axis=1)
        df = df[(df['video_length']!='n/a')&(df['video_length']!='None')&(df['video_length']!='Undefined')]
        df['video_length'] = df['video_length'].astype(int)
        return df

    def tiktok_calc_vtr_rates(self,df):
        df['engagements'] = df['likes'] + df['comments'] + df['shares']
        df['engagement_rate'] = round(df['engagements'] * 100 / df['impressions'], 2)
        df['2s%VTR'] = round(df['ad_video_watched_2_s'] * 100 / df['impressions'], 2)
        df['6s%VTR'] = round(df['ad_video_watched_6_s'] * 100 / df['impressions'], 2)
        df['25%VTR'] = round(df['ad_video_views_p_25'] * 100 / df['impressions'], 2)
        df['50%VTR'] = round(df['ad_video_views_p_50'] * 100 / df['impressions'], 2)
        df['75%VTR'] = round(df['ad_video_views_p_75'] * 100 / df['impressions'], 2)
        df['100%VTR'] = round(df['video_completions'] * 100 / df['impressions'], 2)
        return df
    
    def tiktok_group_by_asset(self,x):
        """"""
        d = {}
        if 'message' in x.columns:
            d['message'] = x['message'].iloc[0]
        d['num_days_ran'] = x.shape[0]
        d['engagements'] = x['engagements'].sum()
        d['impressions'] = x['impressions'].sum()
        d['engagement_rate'] = round(d['engagements'] * 100 / d['impressions'], 2)
        d['2s%VTR'] = round(x['2s%VTR'].mean(), 2)
        d['6s%VTR'] = round(x['6s%VTR'].mean(), 2)
        d['25%VTR'] = round(x['25%VTR'].mean(), 2)
        d['50%VTR'] = round(x['50%VTR'].mean(), 2)
        d['75%VTR'] = round(x['75%VTR'].mean(), 2)
        d['100%VTR'] = round(x['100%VTR'].mean(), 2)
        d['50%VTR_std'] = round(x['50%VTR'].std(), 2)
        d['video_length'] = x['video_length'].iloc[0]
        perc_25_in_secs = round(x['video_length'].iloc[0] * 0.25, 2)
        perc_50_in_secs = round(x['video_length'].iloc[0] * 0.5, 2)
        perc_75_in_secs = round(x['video_length'].iloc[0] * 0.75, 2)
        coordinates = np.array([(2, d['2s%VTR']),(6, d['6s%VTR']),
                            (perc_25_in_secs, d['25%VTR']), (perc_50_in_secs, d['50%VTR']), 
                            (perc_75_in_secs, d['75%VTR']), (d['video_length'], d['100%VTR'])])
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
    
def pickle_data(data, filename,folder="Pickled Files"):
    """Pickle data and save it to a file.
    Args:
        data (Object): The data to be pickled.
        filename (str): The name of the file to save the pickled data to.
        folder (str, optional): The folder to save the pickled file to. Defaults to "Pickled Files"."""

    if os.path.isdir(folder) == False:
        os.mkdir(folder)
    pickle.dump(data, open(folder + '/' + filename, "wb"))