import pandas as pd

def user_stats(df):
    '''

    '''
    # Number of ip addresses per user for each cohort
    df_ip_addresses = df.groupby(by=['cohort_id', 'user_id'])['ip'].nunique().reset_index(1)
    df_ip_addresses.rename(columns={'ip': 'number_of_unique_ips'}, inplace=True)

    # Number of times viewed curriculum
    df_access_events = df.groupby(by=['cohort_id', 'user_id']).page_viewed.count().reset_index(1)
    df_access_events.rename(columns={'page_viewed': 'number_of_access_events'}, inplace=True)

    # Unique pages viewed
    df_unique_content_viewed = df.groupby(by=['cohort_id', 'user_id']).page_viewed.nunique().reset_index(1)
    df_unique_content_viewed.rename(columns={'page_viewed': 'unique_pages_viewed'}, inplace=True)

    # merge dataframes
    ip_access = pd.merge(df_ip_addresses, df_access_events, on=['cohort_id', 'user_id'])
    user_activity = pd.merge(ip_access, df_unique_content_viewed, on=['cohort_id', 'user_id'])

    return user_activity