
from dask import dataframe as dd
from datetime import datetime, timedelta
import logging as lg
import os
from os import scandir
import pandas as pd
import time


logger = lg.getLogger(__name__)
class GapsData:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.gaps_data = dd.DataFrame.from_dict({'file': [], 'Time': [], 'gaps':[], 'init_gap':[], 'end_gap':[]}, npartitions=1)
    
    def search_historical_data(self):
        '''
        Check if historical data exist and define the staring date
        '''
        logger.info('Looking for historical data...')
        try:
            dd.read_json(f'{self.output_data}/*/*.json', orient='records', lines=False)
        except:
            logger.warning(f'NO historical data found!')
            logger.info(f'Running in historical mode...')
            start_date = self.start_date
            return start_date
        else:
            logger.info(f'Historical data found!')
            logger.info(f'Running in append mode...')
            start_date = dd.read_json(f'{self.output_data}/*/*.json', orient='records', lines=False)
            start_date = start_date.sort_values(by='init_gap')
            start_date = start_date['init_gap'].tail(n=1).values[0]
            logger.info(f'Adding data created after {start_date}...')
            return start_date
        
    def get_gap_data(self, entry):
        '''
        Read parquet file and identify gaps
        '''
        logger.info(f'Searching for gaps in file: {entry.path}')
        attempt = 0 
        while attempt < self.max_reads_attemps:
            attempt += 1
            try:
                data = dd.concat([self.gaps_data,
                                  Functions.read_gaps_from_file(entry.path,
                                                                self.minimal_time_gap)]).drop_duplicates().reset_index(drop=True)
                return data
            except:
                logger.warning(f'Cannot read file: {entry.path}')
                if attempt < self.max_reads_attemps:
                    logger.info(f'retry in {self.retry_sleep_time}')
                    time.sleep(self.retry_sleep_time)
                else:
                    logger.error(f'Cannot read file from {self.entry.path}, skiping')
        
    def save_data_by_date(self):
        ### reducing data and spliting by date
        self.gaps_data = self.gaps_data[['file', 'init_gap', 'end_gap']]
        ### saving by day or by date and hour
        if self.split_data_stored_by == 'hour':
            logger.info(f'Saving data by hour')
            floor_val = 'H'
        else:
            logger.info(f'Saving data by day')
            floor_val = 'd'
        ### spliting gaps by date
        for time in dd.to_datetime(self.gaps_data['init_gap']).dt.floor(floor_val).unique():
            t = datetime.strptime(str(time), '%Y-%m-%d %H:%M:%S')
            logger.info(f'Saving gaps data from {t}')
            json_out = self.gaps_data.mask(dd.to_datetime(self.gaps_data['init_gap']).dt.floor(floor_val) != time).dropna()   
            json_out['init_gap'] = json_out['init_gap'].astype(str)
            json_out['end_gap'] = json_out['end_gap'].astype(str)
            self.save_json(json_out, t)

    def save_json(self, json_out, t):
        def name_function(p_index):
            filename = (f'{t.strftime("%Y%m%d")}_{t.strftime("%H%M%S")}.json') 
            # filename = (f'{t.strftime("%Y%m%d")}_{p_index}.json')
            return filename
        outdir = f'{self.output_data}/date={t.strftime("%Y%m%d")}'
        logger.info(f'Writing gaps data into file: {outdir}/{name_function(0)}')
        attempt = 0
        while attempt < self.max_reads_attemps:
            try:
                ### DOES NOT WORK! using dask
                # dd.to_json(json_out, url_path=f'{args["out_path"]}/date={t.strftime("%Y%m%d")}/',
                #            name_function=name_function , orient='records') 
               
                ### IT WORKS! using pandas and creating folders separatedly
                json_out = json_out.compute()
                if not os.path.exists(outdir):
                    logger.info(f'Creating new folder {outdir}')
                    os.makedirs(outdir)
                json_out.to_json(path_or_buf=f'{outdir}/{name_function(0)}',
                                    orient='records', indent=4)
                logger.info(f'Gaps data saved into file: {outdir}/{name_function(0)}')
                break
            except:
                logger.warning(f'Cannot write file: ')
                if attempt < self.max_reads_attemps:
                    logger.info(f'retry in {self.retry_sleep_time}')
                    time.sleep(self.retry_sleep_time)
                else: 
                    logger.error(f'Cannot write file {outdir}/{name_function(0)}, skiping')

               
class Functions:
    def scantree(path):
        ''''
        Recursively yield DirEntry objects for given directory.
        Taken from: https://stackoverflow.com/a/33135143        
        '''
        for entry in scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from Functions.scantree(entry.path)  
            else:
                yield entry

    def check_starting_dates(path, start_date):
            data = dd.read_parquet(path, columns='Time')
            data = data.mask(data < start_date).dropna()
            if data.shape[0].compute() > 0:
                logger.info(f'Found recent data in file {path}')
                return True
            else:
                logger.info(f'No new data found in file {path}')
                return False

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

