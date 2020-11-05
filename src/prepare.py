import pandas as pd
import numpy as np
import re as r


def prep_log_data(df):
    '''
    
    '''
    df.timestamp = df.timestamp.str.replace(r'(\[|\])', '', regex=True)
    df.timestamp = pd.to_datetime(df.timestamp.str.replace(':', ' ', 1)) 
    df = df.set_index('timestamp')
    df.sort_index(inplace=True)
    
    for col in ['request_method', 'request_agent', 'destination']:
        df[col] = df[col].str.replace('"', '')
        
    df['request_method'] = df.request_method.str.replace(r'\?page=[0-9]+', '', regex=True)
    df['agent_name'] = df.request_agent.str.replace(r'([-/ ].*)', "").str.lower()
    df['size_mb'] = [n/1024/1024 for n in df['size']]
    df['download_size'] = pd.cut(df.size_mb, 3, labels=['Low', 'Average', 'High'])
    
    ip_stats = ip_statistics(df)
    df = df.reset_index().merge(ip_stats, left_on='ip', right_on='ip').set_index('timestamp')
    df.index = df.index.tz_localize(None)
    
    return df


def ip_statistics(df):
    '''
    
    '''
    ip_counts = (
    pd.DataFrame(df.ip.value_counts(ascending=False))
    .reset_index()
    .rename(index=str,
            columns={'index': 'ip',
                    'ip': 'ip_count'})
    )


    ip_probabilities = (
    pd.DataFrame(df.ip.value_counts(normalize=True))
    .reset_index()
    .rename(index=str,
            columns={'index': 'ip',
                    'ip': 'ip_probability'})
    )
    
    ip_df = ip_counts.merge(ip_probabilities)
    return ip_df


def ip_status_proabilities(df):
    '''
    
    '''
    status_given_ip = (
    (df.groupby(['ip', 'status']).size()
    / df.groupby(['ip']).status.size())
    .reset_index()
    .rename(columns={0:'prob_status_given_ip'})   
    )
    
    ip_status_count = (
    pd.DataFrame(df.groupby(by=['ip', 'status']).size())
    .reset_index()
    .rename(columns={0:'ip_status_count'})  
    )
    
    ip_status = ip_status_count.merge(status_given_ip, on=['ip', 'status'])
    return ip_status

def eda_log_data(train, df):
    '''
    
    '''
    ip_status = ip_status_proabilities(train)
    
    df = (        
    df
    .reset_index()
    .merge(ip_status, on=['ip', 'status'], how='left')
    .set_index('timestamp')
    )
    df[['request_call','api_version','endpoints','http']] = \
    df.request_method.str.extract(r'(?P<request_call>^[A-Z]+)\s(?P<api_version>\/api\/v[0-9])(?P<endpoints>.+)(?P<http_version>HTTP\/[0-9]\.[0-9])', expand = True)
    
    return df