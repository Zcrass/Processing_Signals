#!/usr/bin/env python

import argparse
import json
import logging as lg
import sys

from utils import GapsData
from utils import Functions as fun

def main():
    gaps = GapsData(**args)
    gaps.start_date = gaps.search_historical_data()
    n_files = 0
    for entry in fun.scantree(gaps.data_path):
        if fun.check_starting_dates(entry, gaps.start_date):
            gaps.gaps_data = gaps.get_gap_data(entry)
        n_files += 1
    ### filter gaps found
    gaps.gaps_data = gaps.gaps_data[gaps.gaps_data['gaps'] == True]
    ### check if gaps where found
    if gaps.gaps_data.shape[0].compute() > 0:
        logger.info(f'Found {gaps.gaps_data.shape[0].compute()} gaps')
        gaps.save_data_by_date()
    else: 
        logger.info(f'No gaps foudn in {n_files+1} files')

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
