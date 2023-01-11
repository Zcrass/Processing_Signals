from dask import dataframe as dd

def subranges(cols, num_sub_ranges, batch_size):
    '''
    Function to compute subranges based on batch size 
    
    Args:
    
    Return:
    '''
    start_list = []
    end_list = []
    for i in range(0, num_sub_ranges):
        start = (i*batch_size)+1
        end = start + batch_size
        if end > len(cols):
            end = len(cols)
        start_list.append(start)
        end_list.append(end)
    return start_list, end_list

def signal_df(df, signal):
    ''' 
    Function to convert df format to output table format 
    
    Args:
    
    Return:
    
    '''
    signal_df = df.dropna()
    signal_df["Date"] = df["Time"].dt.floor("d",).dt.strftime("%Y%m%d")
    signal_df["Signal"] = signal
    names = ["Time", "Value", "Date", "Signal"]
    return signal_df.rename(columns=dict(zip(signal_df.columns, names)))

def check_history(path):
    try:
        dd.read_parquet(path)
    except:
        return True
    else:
        return False

def process_signal(signal, start_date, path):
    ### define columns
    sub_cols = ['Time', signal]
    ### read parquet
    data = dd.read_parquet(path, columns=sub_cols)   
    ### filter by date and round to seconds
    data = data.loc[(data['Time'] >= start_date)]
    data['Time'] = data['Time'].dt.floor('s')
    data = data.groupby(data['Time']).mean().reset_index()
    data = signal_df(data, signal)
    return data
