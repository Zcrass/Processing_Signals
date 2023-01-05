#!/usr/bin/env python

import argparse
from dask import dataframe as dd
from datetime import datetime, timedelta
import logging as lg
import math
import sys

from signal_functions import subranges, signal_df

if __name__ == '__main__':
    ### define logger
    lg.basicConfig(filename='round2sec.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logger = lg.getLogger('round2sec')
    logger.setLevel(lg.INFO)
    
    stdout_handler = lg.StreamHandler(sys.stdout)
    stdout_handler.setLevel(lg.INFO)
    logger.addHandler(stdout_handler)
    
    ### define arguments 
    parser = argparse.ArgumentParser(prog = 'Round2Sec', description = 'Round to a resolution of seconds using mean value from data')
    parser.add_argument('-i', '--input') ### input dir in parquet format
    parser.add_argument('-s', '--start_date') ### start date to process in format
    parser.add_argument('-e', '--end_date') ### end date to process
    parser.add_argument('-v', '--var_cols') ### column names to process
    parser.add_argument('-b', '--batch_size') ### batch size to process
    parser.add_argument('-o', '--out_path') ### batch size to process
    
    args = parser.parse_args()
    
    ########################
    ### define date format
    start_date = datetime.strptime(args.start_date, "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d") 
    ### Historico
    if args.var_cols == "all":
        ### load data with all columns
        cols = list(dd.read_parquet(args.input).columns)
    else:
        #### load data with selected columns
        cols = ["Time"] + var_cols
    logger.info(f'Loading dataframe with {len(cols)} columns...')    
    
    ### define subranges according to batch size
    num_sub_ranges = math.ceil((len(cols)-1)/int(args.batch_size))
    logger.info(f'Processing {num_sub_ranges} batches')
    sub_starts, sub_ends = subranges(cols, num_sub_ranges, int(args.batch_size))

    ### defining naming function
    def name_function(part_ind):
        Date = table["Date"][0]
        name = Date + "_" + format(part_ind, '06d') + ".parquet"
        name =  format(part_ind, '06d') + ".parquet"
        return name
    
    ### run each batch  
    c = 0  
    for i in range(0, num_sub_ranges):
        logger.info(f'Loading batch number: {c+1}')
        ### define subrange of columns
        sub_cols = ["Time"] + cols[sub_starts[i]:sub_ends[i]]
        ### read parquet per batch
        data = dd.read_parquet(args.input, columns=sub_cols)   
        ### filter by date and roun to seconds
        data = data.loc[(data["Time"] >= start_date) & (data["Time"] < (end_date + timedelta(days=1)))]
        data["Time"] = data["Time"].dt.floor("s")
        ### compute means
        mean_df = data.groupby(data["Time"]).mean().reset_index()
        ### compute results table for each column
        for j in sub_cols[1:]:
            c += 1
            table = signal_df(mean_df, j)
            logger.info(f'Saving signal {c}/{len(cols)}: {j}')
            ### save parquet files
            table.to_parquet(path=args.out_path, partition_on=["Date", "Signal"], name_function=name_function)        