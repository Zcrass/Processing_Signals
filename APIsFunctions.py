#!/usr/bin/env python

from datetime import datetime
import json 
from kafka import KafkaProducer
import logging as lg
import pandas as pd
import requests
import time


logger = lg.getLogger(__name__)
class functions:
    
    def read_endpoint(end_point, values):
        '''
        Funtion that retrieves data from JSON API and save as dataframe


        '''
        data = json.loads(requests.get(end_point).text)
        data = pd.json_normalize(data['results'])
        return data[values]
    
    def receive_data(end_point, values, except_values):
        '''
        Functions to try to Connect to API and get data. 
        In case is unable to conect stores a predefined exception data.


        '''
        try: 
            ### Testing conection errors
            ################### REMOVER
            import random
            if random.randint(0,9) > 7:
                raise NameError('Connection Lost')
            ###################
            data = functions.read_endpoint(end_point, values)
        except:
            logger.warning(f'Connection to {end_point} failed...')
            data = pd.DataFrame(except_values)
        data['execution date'] = datetime.now()
        data['execution date'] = data['execution date'].astype(str)
        return(data)

    def send_data(topic_name, data, kafka_out):
        '''
        Function to send data into a kafka service


        '''
        try:
            producer = KafkaProducer(bootstrap_servers=[kafka_out],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            producer.send(topic_name, value=data.to_dict(orient='records'))
        except:
            return False
        else:
            return True


    def generate_data(name, dic, url, values2get, except_values, get_frecuency,
                      submit_frecuency, topic_name, kafka_out,
                      max_length, function_name):
        '''
        Main function to obtain data from an API, concatenate to a dataframe and send it to kafka.

        
        '''
        submittime = starttime = time.time()
        logger.info(f'Starting {name} service...')
        ### starting service
        while True:
            time.sleep(get_frecuency - ((time.time() - starttime) % get_frecuency))
            ### get data from API
            new_data = functions.receive_data(url, values2get, except_values)
            ### data transformation using custom functions if needed
            if function_name != 'None':
                f = getattr(functions, function_name)
                new_data = f(new_data)
            ### concat new data into dataframe
            dic[name] = pd.concat([dic[name], new_data])
            ### check submission frecuency
            if time.time() - submittime >= submit_frecuency:
                logger.info(f'submiting data from {name} to server...')
                ### use submission function
                if functions.send_data(topic_name, dic[name], kafka_out):
                    logger.info(f'{dic[name].shape[0]} rows of data from {name} sended succesfully')
                    dic[name] = pd.DataFrame(columns=values2get)
                else:
                    logger.warning(f'Unable to submit data from {name}, keeping data until next upload')
                submittime = time.time()
            ### check if reached maximum running time
            if (time.time() - starttime) >= max_length:
                logger.info(f'max_length ({max_length} secs) reached. Stoping service {name}')
                break
        # return dic

    def transformation_function(data):
        print('do something to data')
        # data['dob.age'] = data['dob.age'] + 1
        return(data)