#!/usr/bin/env python

import argparse
from dask import dataframe as dd
from datetime import datetime, timedelta
import json
import logging as lg
import sys
import time

from utils import ProcessingSignals

def main():
    signals = ProcessingSignals(**args)
    signals.start_date = signals.search_historical_data_parquet()
    for n, signal_name in enumerate(signals.signals_to_include):
        logger.info(f'Processing signal {n+1}/{len(signals.signals_to_include)}')
        signals.process_signal(signal_name)

if __name__ == '__main__':
    ### define logger
    lg.basicConfig(filename='round2sec.log', filemode='w', level=lg.INFO,
                   format='%(name)s - %(levelname)s - %(message)s')
    logger = lg.getLogger()
    
    stdout_handler = lg.StreamHandler(sys.stdout)
    logger.addHandler(stdout_handler)
    
    # ### define arguments 
    parser = argparse.ArgumentParser(prog = 'round2Sec', description = 'Round to a resolution of seconds using mean value from data')
    parser.add_argument('-i', '--input', help='input json file', required=True) ### input json file
    args = json.load(open(parser.parse_args().input))
    main ()
    
    
    