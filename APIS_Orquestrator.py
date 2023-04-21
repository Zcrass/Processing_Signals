#!/usr/bin/env python

import argparse
import json
import logging as lg
import pandas as pd
import sys
import time
from threading import Thread

from APIsFunctions import functions as fun
import random

if __name__ == '__main__':
    ### define logger
    lg.basicConfig(filename='APIs_Orquestrator.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=lg.INFO)
    logger = lg.getLogger()
    logger.setLevel(lg.INFO)
    
    stdout_handler = lg.StreamHandler(sys.stdout)
    stdout_handler.setLevel(lg.INFO)
    logger.addHandler(stdout_handler)
    
    ### define arguments 
    parser = argparse.ArgumentParser(prog = 'APIsOrquestrator', description = 'Get data from api and send it to kafka')
    parser.add_argument('-i', '--input') ### input json file
    args = json.load(open(parser.parse_args().input))
    max_attemps = args['max_attemps']
    sleep_time = args['sleep_time']

    ########################
    ### defining empty dataframes by endpoint
    lg.info(f'reading {len(args["endpoints"])} endpoint from config file')
    endpoints = {}
    for end in args['endpoints']:
        df = pd.DataFrame()
        endpoints[end['name']] = df
    
    ### add jobs
    threads = []
    logger.info(f'parallelizing request... ')
    for job in args['endpoints']:
        t = Thread(target=fun.generate_data, args=(job['name'], endpoints, job['url'], job['values2get'], job['except_values'], job['get_frecuency'],
                                                   job['submit_frecuency'], job['topic_name'], job['kafka_out'],
                                                   job['max_length'], job['function_name']))
        threads.append(t)

    ### run jobs in parallel
    lg.info(f'Starting services...')
    [t.start() for t in threads]
    ### wait for the threads to finish
    [t.join() for t in threads]

    
   