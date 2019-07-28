# -*- coding: utf-8 -*-
"""
Редактор Spyder

Это временный скриптовый файл.
"""


import configparser
import logging
import requests
import shelve
import json


config = configparser.ConfigParser()
config.read('config.ini')

ENDPOINT_LIST = json.loads(config.get('Section_0', 'ENDPOINT_LIST'))
LOG_FILENAME = config.get('Section_0', 'LOG_FILENAME')
TRANSLATE_KEY = json.loads(config.get('Section_0', 'TRANSLATE_KEY'))
SHELVE_NAME = config.get('Section_0', 'SHELVE_NAME')
URL_COUNTRY_TEMPLATE = config.get('Section_0', 'URL_COUNTRY_TEMPLATE')
URL_DATA_COUNTRY_TEMPLATE = config.get('Section_0', 'URL_DATA_COUNTRY_TEMPLATE')
URL_DATA_COUNTRY_POPULATION_TEMPLATE = config.get('Section_0', 'URL_DATA_COUNTRY_POPULATION_TEMPLATE')
URL_COUNTRY_START = config.getint('Section_0', 'URL_COUNTRY_START')


logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s')


def get_country_list():
    res = list()
    start_country_url = URL_COUNTRY_TEMPLATE.format(URL_COUNTRY_START)
    temp = requests.get(start_country_url)
    if temp.ok:
        try:
            res_temp = temp.json()
            start_page = res_temp['page']
            end_page = res_temp['pages']
            total = res_temp['total']
            res.extend(res_temp['source'][0]['concept'][0]['variable'])
            if start_page < end_page:
                for I in range(2, end_page + 1):
                    country_url = URL_COUNTRY_TEMPLATE.format(I)
                    try:
                        temp = requests.get(country_url)
                        res_temp = temp.json()
                    except Exception as ex:
                        logging.error('ERROR {} occured while getting data by {} endpoint in country endpoint second and etc requests'.format(ex, country_url))  
                    res.extend(res_temp['source'][0]['concept'][0]['variable'])
                if len(res) == total:
                    logging.info('{} totally countru getted from {} countries'.format(len(res), total))
                    return res
            else:
                logging.info('{} totally countru getted from {} countries'.format(len(res), total))
                return res
        except Exception as e:
            logging.error('ERROR {} occured while getting data by {} endpoint in country endpoint data serialize'.format(e, start_country_url))
    else:
        logging.error('ERROR {} occured while getting data by {} endpoint in country endpoint first request'.format(temp.status_code, start_country_url))


def get_data_by_country(list_of_country, list_of_endpoint):
    data_dict_temp = dict()
    for country in list_of_country:
        data_dict_temp[country['id']] = dict()
        for endpoint in list_of_endpoint:
            data_dict_temp[country['id']][endpoint] = dict()
            data_country_url = URL_DATA_COUNTRY_TEMPLATE.format(country['id'], endpoint)
            temp = requests.get(data_country_url)
            if temp.ok:
                res_temp = temp.json()
                if res_temp['page'] == res_temp['pages']:
                    data_dict_temp[country['id']]['lastupdated'] = res_temp['lastupdated']
                    data_dict_temp[country['id']][endpoint]['data'] = res_temp['source']['data']
                else:
                    logging.info('More 1 page in {} endpoint in {} country'.format(endpoint, country))
            else:
                logging.error('ERROR {} occured while getting data by {} endpoint by {} country'.format(temp.status_code, endpoint, country))
    return data_dict_temp


def get_population_by_country(dict_of_data):
    data_dict_temp = dict()
    for country in dict_of_data.keys():
        data_dict_temp[country] = dict()
        url_population = URL_DATA_COUNTRY_POPULATION_TEMPLATE.format(country, URL_COUNTRY_START)
        temp = requests.get(url_population)
        if temp.ok:
            try:
                res_temp = temp.json()
                if res_temp['page'] == res_temp['pages']:
                    for item in res_temp['source']['data']:
                        data_dict_temp[country][item['variable'][2]['id']] = item['value']
                else:
                    for item in res_temp['source']['data']:
                        data_dict_temp[country][item['variable'][2]['id']] = item['value']
                    for I in range(2, res_temp['pages'] + 1):
                        url_population = URL_DATA_COUNTRY_POPULATION_TEMPLATE.format(country, I)
                        temp = requests.get(url_population)
                        if temp.ok:
                            try:
                                res_temp = temp.json()
                                for item in res_temp['source']['data']:
                                    data_dict_temp[country][item['variable'][2]['id']] = item['value']
                            except Exception as es:
                                logging.error('Error {} accured for {} country id at url {}'.format(es, country, url_population))
            except Exception as e:
                logging.error('Error {} accured for {} country id at url {}'.format(e, country, url_population))
    return data_dict_temp


def aggregation_data_by_country(dict_of_data, synonyms):
    res_doc_dict = dict()
    for key, value in dict_of_data.items():
        if key != 'lastupdated':
            res_doc_dict[key] = dict()
            for key_0, value_0 in value.items():
                if key_0 != 'lastupdated':
                    for item in value_0['data']:
                        if item['variable'][2]['id'] in res_doc_dict[key].keys():
                            key_syn = synonyms.get(item['variable'][1]['value'], 'No_value')
                            res_doc_dict[key][item['variable'][2]['id']][key_syn] = item['value']
                            res_doc_dict[key][item['variable'][2]['id']]['country_risk'] += float(0 if item['value'] is None else item['value'])
                        else:
                            res_doc_dict[key][item['variable'][2]['id']] = dict()
                            key_syn = synonyms.get(item['variable'][1]['value'], 'No_value')
                            res_doc_dict[key][item['variable'][2]['id']][key_syn] = item['value']
                            res_doc_dict[key][item['variable'][2]['id']]['year'] = item['variable'][2]['value']
                            res_doc_dict[key][item['variable'][2]['id']]['country_code'] = item['variable'][0]['id']
                            res_doc_dict[key][item['variable'][2]['id']]['country'] = item['variable'][0]['value']
                            res_doc_dict[key][item['variable'][2]['id']]['lastupdated'] = value['lastupdated']
                            res_doc_dict[key][item['variable'][2]['id']]['country_risk'] = float(0 if item['value'] is None else item['value'])
    return res_doc_dict


def produce_output(country_data, country_population):
    for key, value in country_data.items():
        for key_0, value_0 in value.items():
            country_data[key][key_0]['population'] = (country_population.get(key, {})).get(key_0)
    return country_data


def creat_on_disk(data, shelve_name):
    '''
    Save data to shelve
    by country keys
    '''
    db = shelve.open(shelve_name)
    for key in data:
        db[key] = data[key]
    db.close()


if __name__ == "__main__":

    country_list = get_country_list()
    data_dict = get_data_by_country(country_list, ENDPOINT_LIST)
    country_population_dict = get_population_by_country(data_dict)
    res_data_dict = aggregation_data_by_country(data_dict, TRANSLATE_KEY)
    final_data_dict = produce_output(res_data_dict, country_population_dict)

    creat_on_disk(final_data_dict, SHELVE_NAME)



#                print('ERROR {} occured while getting data by {} endpoint by {} country'.format(temp.status_code, endpoint, country))
#                    print('More 1 page in {} endpoint in {} country'.format(endpoint, country))
#                    data_dict[country['id']][endpoint]['data'].append(res_temp['source']['data'])
#                    lastupdate = res_temp['lastupdated']
#                    data_dict[country].extend(res_temp['source']['data'])
#                    data_dict[country].extend(res_temp['source']['data'])
#                                data_dict[country].extend(res_temp['source']['data'])
#                                print('Error {} accured for {} country id at url {}'.format(es, country, url_population))
#                            print(item['variable'][1]['value'])
#                            print(item['variable'][1]['value'])
#                        print(item['variable'][2]['id'])
#                            res_doc_dict[key][item['variable'][2]['id']][item['variable'][1]['value']] = item['value']
#                            res_doc_dict[key][item['variable'][2]['id']][item['variable'][1]['value']] = item['value']
#                print('Error {} accured for {} country id at url {}'.format(e, country, url_population))    
#    logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)        
#            data_dict[country['id']][endpoint]['data'] = list()
#ENDPOINT_LIST = ['CC.EST', 'GE.EST', 'PV.EST', 'RQ.EST', 'RL.EST', 'VA.EST']
#LOG_FILENAME = 'C:\\Users\Z240\\world_bank\\get_data.log'
#TRANSLATE_KEY = {'Control of Corruption: Estimate': 'control_of_corruption_estimate',
#                 'Government Effectiveness: Estimate': 'government_effectiveness_estimate',
#                 'Political Stability and Absence of Violence/Terrorism: Estimate': 'political_stability_and_absence_of_violence_estimate',
#                 'Regulatory Quality: Estimate': 'regulatory_quality_estimate',
#                 'Rule of Law: Estimate': 'rule_of_law_estimate',
#                 'Voice and Accountability: Estimate': 'voice_and_accountability_estimate'
#                 }
#SHELVE_NAME = 'countries_data'
#URL_COUNTRY_TEMPLATE = 'https://api.worldbank.org/v2/sources/3/country?page={}&format=json'
#URL_DATA_COUNTRY_TEMPLATE = 'https://api.worldbank.org/v2/sources/3/country/{}/series/{}?format=json'
#URL_DATA_COUNTRY_POPULATION_TEMPLATE = 'https://api.worldbank.org/v2/sources/2/country/{}/series/SP.POP.TOTL?format=json&page={}'
#                                        'https://api.worldbank.org/v2/sources/2/country/ABW/series/SP.POP.TOTL?format=json&page=2'
#URL_COUNTRY_START = 1       