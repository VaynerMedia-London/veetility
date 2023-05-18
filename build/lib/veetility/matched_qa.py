
#%%
import pandas as pd
import numpy as np

def identify_match_multi_cols(df_1, df_2, df_1_cols_to_match, df_2_cols_to_match, match_col_name, exclude_values=['None', 'none', 'nan', '']):
    
    def is_row_in_dataframe(row, target_df, source_cols, target_cols):
        mask = np.full(len(target_df), True, dtype=bool)
        for i in range(len(source_cols)):
            current_condition = (
                (target_df[target_cols[i]] == row[source_cols[i]]) &
                (row[source_cols[i]] not in exclude_values)
            ) | (
                pd.isna(target_df[target_cols[i]]) & pd.isna(row[source_cols[i]])
            )
            mask &= current_condition
        matches = target_df[mask]
        return len(matches) > 0

    df_1[match_col_name + '_df1?'] = df_1.apply(lambda row: is_row_in_dataframe(row, df_2, df_1_cols_to_match, df_2_cols_to_match), axis=1)
    df_2[match_col_name + '_df2?'] = df_2.apply(lambda row: is_row_in_dataframe(row, df_1, df_2_cols_to_match, df_1_cols_to_match), axis=1)

    return df_1, df_2

# Sample data
data1 = {'Platform': ['Facebook', 'Twitter',     'Instagram',    '',       ''],
         'Message': ['hello there', 'how are you', 'nice pic', 'test',   'hello']}
df_1 = pd.DataFrame(data1)

data2 = {'Platform': ['Facebook', 'Twitter', 'LinkedIn',          'None',  ''],
         'Message': ['hello there', 'whats up', 'connect with me', 'test',   'hello']}
df_2 = pd.DataFrame(data2)

# Call the function
df_1, df_2 = identify_match_multi_cols(df_1, df_2, ['Platform', 'Message'], ['Platform', 'Message'], 'Matched')

print(df_1)
print(df_2)


# %%
