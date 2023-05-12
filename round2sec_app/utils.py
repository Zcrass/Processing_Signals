import logging as lg
from dask import dataframe as dd
import time


logger = lg.getLogger(__name__)
class ProcessingSignals():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def search_historical_data_parquet(self):
        '''
        Check if historical data exist and define the staring date
        '''
        logger.info('Looking for historical data...')
        try:
            dd.read_parquet(self.output_data_path)
        except:
            logger.warning(f'NO historical data found!')
            start_date = self.start_reading_date
            logger.info(f'Running in historical mode starting from date: {start_date}...')
            return start_date
        else:
            logger.info(f'Historical data found!')
            start_date = dd.read_parquet(self.output_data_path)
            start_date = start_date.sort_values(by='Time')
            start_date = start_date['Time'].tail(n=1).values[0]
            logger.info(f'Running in append mode...')
            logger.info(f'Adding data created after {start_date}...')
            return start_date
        
    def process_signal(self, signal_name):
        '''
        Read, transform and save data for individual signal
        '''
        logger.info(f'Processing signal: {signal_name}...')
        signal_data = SignalData(signal_name, **self.__dict__)
        ### read data from signal
        signal_data.input_data = signal_data.read_parquet_signal()
        ### transform signal data
        signal_data.output_data = signal_data.signal_table_formater()
        ### save signal data
        signal_data.write_parquet_table()
        logger.info(f'Signal: {signal_name} processed succesfully!')
        
class SignalData():
    def __init__(self, signal_name, **kwargs):
        self.__dict__ = kwargs
        self.signal_name = signal_name
        self.columns = ['Time', self.signal_name]
 
    def read_parquet_signal(self):
        '''
        Read signal data from parquet file
        '''
        logger.info(f'reading signal {self.signal_name} from path {self.input_data_path}')
        attempt = 0
        while attempt < self.max_reads_attemps:
            attempt += 1
            try:  
                data = dd.read_parquet(self.input_data_path, columns=self.columns)
                ### filter by date and round to seconds
                data = data.loc[(data['Time'] >= self.start_reading_date)]
                data['Time'] = data['Time'].dt.floor('s')
                data = data.groupby(data['Time']).mean().reset_index()
                return data
            except:
                logger.warning(f'Cannot read file: {self.input_data_path}')
                if attempt < self.max_reads_attemps:
                    logger.info(f'retry in {self.retry_sleep_time}')
                    time.sleep(self.retry_sleep_time)
                else:
                    logger.error(f'Cannot read signal {self.signal_name} from path {self.input_data_path}')
                
    def signal_table_formater(self):
        ''' 
        Function to convert df format to output table format 
        '''
        output_data = self.input_data.dropna()
        output_data["Date"] = self.input_data["Time"].dt.floor("d",).dt.strftime("%Y%m%d")
        output_data["Signal"] = self.signal_name
        names = ["Time", "Value", "Date", "Signal"]
        return output_data.rename(columns=dict(zip(output_data.columns, names)))
    
    def write_parquet_table(self):
        '''
        Saves parquet data into ouput path
        '''
        def name_function(part_ind):
            return f'{day}_{str(part_ind).zfill(6)}.parquet'
        
        logger.info(f'Saving signal: {self.signal_name} starting from date {self.start_reading_date}...')
        attempt = 0
        while attempt < self.max_reads_attemps:
            attempt += 1
            try:
                ### save by date
                for day in self.output_data.Date.unique():
                    ### save parquet files
                    self.output_data.loc[self.output_data.Date == day].to_parquet(path=self.output_data_path,
                                                                                partition_on=['Date', 'Signal'], 
                                                                                name_function=name_function)
                break  
            except:
                logger.warning(f'Cannot write file into: {self.output_data_path}')
                if attempt < self.max_reads_attemps:
                    logger.info(f'retry in {self.retry_sleep_time}')
                    time.sleep(self.retry_sleep_time)
                else:
                    logger.error(f'Cannot write signal {self.signal_name} into path {self.output_data_path} skiping')



