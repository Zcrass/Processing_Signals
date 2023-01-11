#!/usr/bin/env python

import argparse
from dask import dataframe as dd
from datetime import datetime, timedelta
import json
import logging as lg
import math
import sys

from signal_functions import check_history, process_signal

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
    
    ########################
    ### define date format
    # start_date = datetime.strptime(str(args['start_date']), '%Y%m%d')
    cols = args['var_cols'] 
    logger.info(f'Loading dataframe with {len(cols)} columns...')    
    
    ### defining naming function
    def name_function(part_ind):
            return f'{day}_{str(part_ind).zfill(6)}.parquet'
        
    if check_history(args['out_path']):
        ### running in historical mode
        logger.warning(f'NO historical data found!')
        logger.info(f'Running in historical mode...')
        for c, col in enumerate(cols):
            ### iterate through signals
            start_date = datetime.strptime(str(args['start_date']), '%Y%m%d')
            logger.info(f'Processing signal: {c+1}/{len(cols)}')
            data = process_signal(col, start_date, args['input'])
            logger.info(f'Saving signal: {col} starting in date {start_date}...')
            ### save by date
            for day in data.Date.unique():
                ### save parquet files
                data.loc[data.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        
            logger.info(f'Added {data.shape[0].compute()} new rows for signal: {col}')
    else:
        ### running in append mode
        logger.info(f'Historical data found!')
        stored_data = dd.read_parquet(args['out_path'])
        for c, col in enumerate(cols):
            ### iterate through signals
            logger.info(f'Processing signal: {c+1}/{len(cols)}')
            if col in list(stored_data.Signal.unique().compute()):
                # logger.info(f'Signal {col} already in stored data!')
                old_date = stored_data.loc[stored_data.Signal == col]['Time'].max().compute().floor("d",)
                new_date = dd.read_parquet(args['input'], columns='Time').max().compute().floor("d",)
                if new_date > old_date:
                    start_date = old_date + timedelta(days=1)
                    data = process_signal(col, start_date, args['input'])
                    logger.info(f'Adding new data for signal: {col} starting in date {start_date}')
                    for day in data.Date.unique():
                        ### save parquet files
                        data.loc[data.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        
                    logger.info(f'Added {data.shape[0].compute()} new rows for signal {col}')
                else:
                    logger.info(f'No new data added for signal: {col}')
            else:
                start_date = datetime.strptime(str(args['start_date']), '%Y%m%d')
                data = process_signal(col, start_date, args['input'])
                logger.info(f'Saving data for new signal: {col} starting in date {start_date}')
                for day in data.Date.unique():
                    ### save parquet files
                    data.loc[data.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        
                logger.info(f'Added {data.shape[0].compute()} new rows for signal {col}')
                

                    