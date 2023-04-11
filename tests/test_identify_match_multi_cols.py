from veetility.utility_functions import UtilityFunctions
import unittest
import pandas as pd
import numpy as np


class TestIdentifyMatchMultiCols(unittest.TestCase):

    def setUp(self):
        self.util_class = UtilityFunctions()

    def test_identify_match_multi_cols_no_matches(self):
        df1 = pd.DataFrame({'A': [1, 2, 3], 'B': ['one', 'two', 'three']})
        df2 = pd.DataFrame({'C': [4, 5, 6], 'D': ['four', 'five', 'six']})
        result_df1, result_df2 = self.util_class.identify_match_multi_cols(df1, df2, ['A', 'B'], ['C', 'D'], 'match')
        self.assertFalse(result_df1['match_df1?'].any())
        self.assertFalse(result_df2['match_df2?'].any())

    def test_identify_match_multi_cols_with_matches(self):
        df1 = pd.DataFrame({'A': [1, 2, 3], 'B': ['one', 'two', 'three']})
        df2 = pd.DataFrame({'C': [1, 4, 3], 'D': ['one', 'four', 'three']})
        result_df1, result_df2 = self.util_class.identify_match_multi_cols(df1, df2, ['A', 'B'], ['C', 'D'], 'match')
        self.assertTrue(result_df1['match_df1?'].iloc[0])
        self.assertFalse(result_df1['match_df1?'].iloc[1])
        self.assertTrue(result_df1['match_df1?'].iloc[2])
        self.assertTrue(result_df2['match_df2?'].iloc[0])
        self.assertFalse(result_df2['match_df2?'].iloc[1])
        self.assertTrue(result_df2['match_df2?'].iloc[2])
    
    def test_identify_match_multi_cols_exclude_values(self):
        df1 = pd.DataFrame({'A': [1, 2, 3, np.nan, 'None'], 'B': ['one', 'two', 'three', 'nan', 'none']})
        df2 = pd.DataFrame({'C': [1, 4, 3, np.nan, 'None'], 'D': ['one', 'four', 'three', 'nan', 'none']})
        exclude_values = ['None', 'none', 'nan', '']
        result_df1, result_df2 = self.util_class.identify_match_multi_cols(df1, df2, ['A', 'B'], ['C', 'D'], 'match', exclude_values)
        
        self.assertTrue(result_df1['match_df1?'].iloc[0])
        self.assertFalse(result_df1['match_df1?'].iloc[1])
        self.assertTrue(result_df1['match_df1?'].iloc[2])
        self.assertFalse(result_df1['match_df1?'].iloc[3])
        self.assertFalse(result_df1['match_df1?'].iloc[4])
        
        self.assertTrue(result_df2['match_df2?'].iloc[0])
        self.assertFalse(result_df2['match_df2?'].iloc[1])
        self.assertTrue(result_df2['match_df2?'].iloc[2])
        self.assertFalse(result_df2['match_df2?'].iloc[3])
        self.assertFalse(result_df2['match_df2?'].iloc[4])


if __name__ == '__main__':
    unittest.main()