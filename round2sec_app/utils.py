from dask import dataframe as dd
import pandas as pd
from os import scandir

class functions:
    def check_history(path):
        '''
        
        '''
        try:
            dd.read_parquet(path)
        except:
            return True
        else:
            return False
                    
    def process_signal(signal, start_date, path):
        '''
        
        '''
        ### define columns
        sub_cols = ['Time', signal]
        ### read parquet
        data = dd.read_parquet(path, columns=sub_cols)   
        ### filter by date and round to seconds
        data = data.loc[(data['Time'] >= start_date)]
        data['Time'] = data['Time'].dt.floor('s')
        data = data.groupby(data['Time']).mean().reset_index()
        data = aditional_functions.signal_df(data, signal)
        return data

    def scantree(path):
        ''''
        Recursively yield DirEntry objects for given directory.
        Taken from: https://stackoverflow.com/a/33135143        
        '''
        for entry in scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from functions.scantree(entry.path)  
            else:
                yield entry

    def read_gaps_from_file(path, time_gap):
        '''
            identify time gaps in  parquet file
            
            Args: 
                path: path to parquet file
            
            Return:
                A dask data frame with three columns: [file, Time, gaps]
        '''
        df = dd.DataFrame.from_dict({'file': [], 'Time': [], 'gaps':[], 'init_gap':[], 'end_gap':[]}, npartitions=1)
        df['Time'] = dd.read_parquet(path, columns='Time').dt.floor('s').unique()
        df = df.sort_values(by="Time")
        df['gaps'] = df['Time'].diff() > pd.to_timedelta(time_gap)
        df['gaps'] = df['gaps'].astype(dtype='object')
        df['end_gap'] = df['Time'].mask(df['gaps'] != True)
        df['init_gap'] = df['Time'].shift(1).mask(df['gaps'] != True)
        df['file'] = path
        return(df)

    def check_dates(path, start_date):
        if start_date != None:
            data = dd.read_parquet(path, columns='Time')
            data = data.mask(data < start_date).dropna()
            if data.shape[0].compute() > 0:
                return True
            else:
                return False
        else:
            return True
class aditional_functions:     
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

