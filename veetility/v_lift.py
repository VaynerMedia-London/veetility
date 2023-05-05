import pandas as pd
# import psycopg2
import numpy as np


def add_on_weight_v2(x,inflection,w):
    """Calculate the add-on weight of the engagement metric using beta hill function
    if the value is higher than the threshold, return the logit transformed weight, else return 0
    
    Args:
        x (float): the engagement metric value
        inflection (float): the inflection point of the beta hill function
        w (float): the weight of the engagement metric
    
    Returns:
        w_value (float): the add-on weight of the engagement metric"""
    hill=1
    #print(f'inflection: {inflection}, w: {w}, x: {x}')

    if x==0:
        w_value=0
    else:
        w_value=1/(1+(x/inflection)**(-hill)) * w
    return  w_value
    

def v_lift(df,metric_list,impression_weight):
    """Calculate the lift value for the engagement columns, 
    Return a dataframe with all original columns and three results columns: ER, Score and V_lift
    
    Args:
        df (dataframe): a dataframe with columns: 'impressions', 'engagement metric 1', 'engagement metric 2', ...
        metric_list (list): a list of engagement metric column names
        impression_weight (float): the weight of impression in the final score
    
    Returns:
        df (dataframe): a dataframe with all original columns and three results columns: ER, Score and V_lift"""

    df.rename(columns={'impressions':'Impressions'},inplace=True)
    impression_metric_list=['Impressions']+metric_list
    
    metrics_avg=[]
    inv_avg=[]
    add_on_weight_col=[]

    impression_metrics_avg=[df['Impressions'].mean()]
    for column in metric_list:
      metrics_avg.append(df[column].mean())
      impression_metrics_avg.append(df[column].mean())
    
    inv_avg=[0 if x==0 else 1/x for x in metrics_avg]
    metric_weight=[x/sum(inv_avg) for x in inv_avg]

    impression_metric_weight=list(np.array(metric_weight)*(1-impression_weight))
    impression_metric_weight.insert(0,impression_weight)

    # V_lift calculation
    for i, x in enumerate(impression_metric_list):
      df['weight_'+x] = df[x].apply(lambda row: add_on_weight_v2(row,impression_metrics_avg[i],impression_metric_weight[i])) # v2 add on weight  
      add_on_weight_col.append('weight_'+x)
      
    df['ER']=df[metric_list].sum(axis=1)/df['Impressions']
    df['V_Lift']=df[add_on_weight_col].sum(axis=1)   
    
    return df, impression_metric_weight