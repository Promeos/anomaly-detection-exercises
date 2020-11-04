import numpy as np
import pandas as pd


def get_log_data():
    '''
    
    '''
    column_names=['ip', 'timestamp', 'request_method', 'status',
              'size', 'destination', 'request_agent']

    df_request = pd.read_csv('http://python.zach.lol/access.log',          
                    engine='python',
                    header=None,
                    index_col=False,
                    names=column_names,
                    sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])',
                    na_values='"-"',
                    usecols=[0, 3, 4, 5, 6, 7, 8]
    )

    new = pd.DataFrame([["95.31.18.119", "[21/Apr/2019:10:02:41+0000]", 
                        "GET /api/v1/items/HTTP/1.1", 200, 1153005, np.nan, 
                        "python-requests/2.21.0"],
                        ["95.31.16.121", "[17/Apr/2019:19:36:41+0000]", 
                        "GET /api/v1/sales?page=79/HTTP/1.1", 301, 1005, np.nan, 
                        "python-requests/2.21.0"],
                        ["97.105.15.120", "[18/Apr/2019:19:42:41+0000]", 
                        "GET /api/v1/sales?page=79/HTTP/1.1", 301, 2560, np.nan, 
                        "python-requests/2.21.0"],
                        ["97.105.19.58", "[19/Apr/2019:19:42:41+0000]", 
                        "GET /api/v1/sales?page=79/HTTP/1.1", 200, 2056327, np.nan, 
                        "python-requests/2.21.0"]], columns=column_names)
    
    df = df_request.append(new)
    return df
