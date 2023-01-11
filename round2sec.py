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
    # parser.add_argument('-s', '--start_date') ### start date to process in format
    # parser.add_argument('-e', '--end_date') ### end date to process
    # parser.add_argument('-v', '--var_cols') ### column names to process
    # parser.add_argument('-b', '--batch_size') ### batch size to process
    # parser.add_argument('-o', '--out_path') ### batch size to process
    
    # args = parser.parse_args()
    args = json.load(open(parser.parse_args().input))
    
    ########################
    ### define date format
    start_date = datetime.strptime(str(args['start_date']), '%Y%m%d')
    # end_date = datetime.strptime(str(args['end_date']), '%Y%m%d') 
        
            # try:
            #     dd.read_parquet(args['out_path', columns='Time'])
            ### compare star dates to check if historical is correct
            #     print('compare start dates')
            #     print(stored_dates)
    cols = args['var_cols'] 
    logger.info(f'Loading dataframe with {len(cols)} columns...')    
    
    ### defining naming function
    def name_function(part_ind):
            return f'{day}_{str(part_ind).zfill(6)}.parquet'
        
    if check_history(args['out_path']):
        logger.warning(f'NO historical data found!')
        logger.info(f'Running in historical mode')
        # if args['var_cols'] == 'all':
        #     ### load data with all columns
        #     cols = list(dd.read_parquet(args['input']).columns)
        # else:
        #     #### load data with selected columns
        #     cols = ['Time'] + args['var_cols'] 
        ### define subranges according to batch size
        # num_sub_ranges = math.ceil((len(cols)-1)/int(args['batch_size']))
        # logger.info(f'Processing {num_sub_ranges} batches')
        # sub_starts, sub_ends = subranges(cols, num_sub_ranges, int(args['batch_size']))


            
        for c, col in enumerate(cols):
            start_date = start_date - timedelta(days=1)
            data = process_signal(col, start_date, args['input'])
            logger.info(f'Saving signal {c+1}/{len(cols)}: {col}')
            ### save by date
            for day in data.Date.unique():
                ### save parquet files
                data.loc[data.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        

        # ### run each batch    
        # c = 0  
        # for batch in range(0, num_sub_ranges):
        #     logger.info(f'Loading batch number: {batch+1}')
        #     ### define subrange of columns
        #     sub_cols = ['Time'] + cols[sub_starts[batch]:sub_ends[batch]]
        #     ### read parquet per batch
        #     data = dd.read_parquet(args['input'], columns=sub_cols)   
        #     ### filter by date and roun to seconds
        #     data = data.loc[(data['Time'] >= start_date)]
        #     data['Time'] = data['Time'].dt.floor('s')
        #     ### compute means
        #     data = data.groupby(data['Time']).mean().reset_index()
        #     ### compute results table for each column
        #     for col in sub_cols[1:]:
        #         c += 1
        #         table = signal_df(data, col)
        #         logger.info(f'Saving signal {c}/{len(cols)}: {col}')
        #         ### save by date
        #         for day in table.Date.unique():
        #             ### save parquet files
        #             table.loc[table.Date == day].to_parquet(path=args['out_path'], partition_on=['Date', 'Signal'], name_function=name_function)        
    else:
        logger.info(f'Historical data found!')
        stored_data = dd.read_parquet(args['out_path'])
        # stored_signals = list(stored_data.Signal.unique().compute())
        
        for col in cols:
            if col in list(stored_data.Signal.unique().compute()):
                logger.info(f'Signal {col} already in stored data')
                stored_date = stored_data.loc[stored_data.Signal == col]['Time'].max().compute().floor("d",).strftime("%Y%m%d")
                data = data.loc[(data['Time'] > stored_date)]
                # stored_date = datetime.strptime(str(stored_date), '%Y%m%d')
                # dd.read_parquet(args['out_path']).Signal.unique().compute()
                print(stored_date)
                # if start_date <= 
            else:
                logger.info(f'found new signal: {col}')
                
            

                    