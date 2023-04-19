#!/usr/bin/env python

import argparse
from dask import dataframe as dd
from datetime import datetime, timedelta
import json
import logging as lg
import sys
import time

from signal_functions import functions as fun

if __name__ == '__main__':
    ### define logger
    lg.basicConfig(filename='round2sec.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logger = lg.getLogger('round2sec')
    logger.setLevel(lg.INFO)
    
    stdout_handler = lg.StreamHandler(sys.stdout)
    stdout_handler.setLevel(lg.INFO)
    logger.addHandler(stdout_handler)
    
    # ### define arguments 
    parser = argparse.ArgumentParser(prog = 'Round2Sec', description = 'Round to a resolution of seconds using mean value from data')
    parser.add_argument('-i', '--input') ### input json file
    args = json.load(open(parser.parse_args().input))
    max_attemps = args["max_attemps"]
    sleep_time = args['sleep_time']
    cols = args['var_cols'] 

    ########################
    logger.info(f'Loading dataframe with {len(cols)} columns...')        
    ### defining naming function
    def name_function(part_ind):
            return f'{day}_{str(part_ind).zfill(6)}.parquet'
    if fun.check_history(args['out_path'], format='parquet'):
        ### running in historical mode
        logger.warning(f'NO historical data found!')
        logger.info(f'Running in historical mode...')
        for c, col in enumerate(cols):
            logger.info(f'{c+1}/{len(cols)} Processing signal: {col}...')
            for attempt in range(max_attemps+1):
                if attempt < max_attemps:
                    ### iterate through signals
                    start_date = datetime.strptime(str(args['start_date']), '%Y%m%d')
                    try:        
                        data = fun.process_signal(col, start_date, args['input'])
                        logger.info(f'Saving signal: {col} starting from date {start_date}...')
                        ### save by date
                        for day in data.Date.unique():
                            ### save parquet files
                            data.loc[data.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        
                    except:
                        logger.warning(f'Attempt number {attempt+1} failed. retry in {sleep_time} sec...')
                        time.sleep(sleep_time)
                    else:
                        logger.info(f'Added {data.shape[0].compute()} new rows for signal: {col}')
                        break
                else:
                    logger.error(f'ERROR: processing signal {col} failed! skipping...')
    else:
        ### running in append mode
        logger.info(f'Historical data found!')
        stored_data = dd.read_parquet(args['out_path'])
        for c, col in enumerate(cols):
            ### iterate through signals
            logger.info(f'{c+1}/{len(cols)} Processing signal: {col}...')
            ### check if signal is already on stored data
            if col in list(stored_data.Signal.unique().compute()):
                ### check if newer data is available for signal
                old_date = stored_data.loc[stored_data.Signal == col]['Time'].max().compute().floor("d",)
                new_date = dd.read_parquet(args['input'], columns='Time').max().compute().floor("d",)
                if new_date > old_date:
                    start_date = old_date + timedelta(days=1)
                    logger.info(f'Adding new data to signal {col} starting from date {start_date}')
                else:
                    logger.info(f'No new data added for signal: {col}')
                    continue
            else:
                start_date = datetime.strptime(str(args['start_date']), '%Y%m%d')
                logger.info(f'Saving data for new signal {col} starting in date {start_date}')
            
            for attempt in range(max_attemps+1):
                if attempt < max_attemps:
                    try:
                        data = fun.process_signal(col, start_date, args['input'])
                        for day in data.Date.unique():
                            ### save parquet files
                            data.loc[data.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        
                    except:
                        logger.warning(f'Attempt number {attempt+1} failed. retry in {sleep_time} sec...')
                        time.sleep(sleep_time)
                    else:
                        logger.info(f'Added {data.shape[0].compute()} new rows for signal {col}')
                        break
                else:
                    logger.error(f'ERROR: processing signal {col} failed! skipping...')

                    