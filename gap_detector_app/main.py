#!/usr/bin/env python

import argparse
from dask import dataframe as dd
from datetime import datetime, timedelta
import json
import logging as lg
import os
import pandas as pd
import sys
import time

from utils import GapsData, StoredData

def main():
    gaps = GapsData(**args)
    gaps.start_date = gaps.search_historical_data()
    
    data = StoredData(gaps.input_data)
    for entry in data.scantree():
        print(entry)






if __name__ == '__main__':
    ### define logger
    lg.basicConfig(filename='gaps.log', filemode='w', level=lg.INFO,
                   format='%(name)s - %(levelname)s - %(message)s')
    logger = lg.getLogger()  
    stdout_handler = lg.StreamHandler(sys.stdout)
    logger.addHandler(stdout_handler)
    
    ### define arguments 
    parser = argparse.ArgumentParser(prog = 'gapsDetector', description = 'Identify and report time gaps')
    parser.add_argument('-i', '--input') ### input json file
    args = json.load(open(parser.parse_args().input))

    main()
    ########################


    # logger.info(f'Loading dataframe...')
    # for attempt in range(max_attemps+1):
    #     if attempt < max_attemps:
    #         logger.info(f'Reading parquet data from path: {args["input"]}')
    #         try:
    #             ### create empty dataframe
    #             data = dd.DataFrame.from_dict({'file': [], 'Time': [], 'gaps':[], 'init_gap':[], 'end_gap':[]}, npartitions=1)
    #             data['gaps'] = data['gaps'].astype(dtype='object')
    #             ### read individual parquet file 
    #             for n, entry in enumerate(gaps.scantree(args["input"])):
    #                 ### check starting date
    #                 if fun.check_dates(entry, start_date):
    #                     ### find gaps and store values in data
    #                     logger.info(f'Searching for gaps in file: {entry.path}')
    #                     data = dd.concat([data, fun.read_gaps_from_file(entry.path, args['time_gap'])]).drop_duplicates().reset_index(drop=True)
    #                 else:
    #                     logger.info(f'No new data in file: {entry.path}')
    #             ### filter gaps found
    #             data = data.loc[data['gaps'] == True]
    #             ### check if gaps where found
    #             if data.shape[0].compute() > 0:
    #                 logger.info(f'Found {data.shape[0].compute()} gaps in {n+1} files')
    #                 ### reducing data and spliting by date
    #                 data = data[['file', 'init_gap', 'end_gap']]

    #                 ### saving by day or by date and hour
    #                 if args['store_by'] == 'hour':
    #                     logger.info(f'Saving data by hour')
    #                     floor_val = 'H'
    #                 elif args['store_by'] == 'day':
    #                     floor_val = 'd'
    #                     logger.info(f'Saving data by day')
    #                 ### spliting gaps by date
    #                 for time in dd.to_datetime(data['init_gap']).dt.floor(floor_val).unique():
    #                     t = datetime.strptime(str(time), '%Y-%m-%d %H:%M:%S')
                        
    #                     logger.info(f'Saving gaps data from {t}')
    #                     json_out = data.mask(dd.to_datetime(data['init_gap']).dt.floor(floor_val) != time).dropna()   
    #                     json_out['init_gap'] = json_out['init_gap'].astype(str)
    #                     json_out['end_gap'] = json_out['end_gap'].astype(str)

    #                     def name_function(p_index):
    #                         filename = (f'{t.strftime("%Y%m%d")}_{t.strftime("%H%M%S")}.json') 
    #                         # filename = (f'{t.strftime("%Y%m%d")}_{p_index}.json')
    #                         return filename
                        
    #                     ### DOES NOT WORK! using dask
    #                     # dd.to_json(json_out, url_path=f'{args["out_path"]}/date={t.strftime("%Y%m%d")}/',
    #                     #            name_function=name_function , orient='records') 

    #                     ## IT WORKS! using pandas and creating folders separatedly
    #                     json_out = json_out.compute()
    #                     outdir = f'{args["out_path"]}/date={t.strftime("%Y%m%d")}/'
    #                     if not os.path.exists(outdir):
    #                         logger.info(f'Creating new folder {outdir}')
    #                         os.makedirs(outdir)
    #                     json_out.to_json(path_or_buf=f'{args["out_path"]}/date={t.strftime("%Y%m%d")}/{name_function(0)}',
    #                                      orient='records', indent=4)
    #                     logger.info(f'Gaps data saved into file: {args["out_path"]}/date={t.strftime("%Y%m%d")}/{name_function(0)}')
    #             else:
    #                 logger.info(f'No gaps where found')                
    #         except:
    #             logger.warning(f'Attempt number {attempt+1} failed. retry in {sleep_time} sec...')
    #             time.sleep(sleep_time)
    #         else:
    #             logger.info(f'Parquet data analized succesfully')
    #             break
    #     else:
    #         logger.error(f'ERROR: cannot read data...')



