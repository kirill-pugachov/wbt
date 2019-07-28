# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 09:36:30 2019

@author: Z240
"""

import configparser
import shelve
import logging


config = configparser.ConfigParser()
config.read('config.ini')

SHELVE_NAME = config.get('Section_0', 'SHELVE_NAME')
LOG_FILENAME = config.get('Section_0', 'LOG_FILENAME')

logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s')

def get_country_list(shelve_name):
    '''
    Get keys from external storage
    at shelve_name
    '''
    try:
        db = shelve.open(shelve_name)
        a = list(db.keys())
        db.close()
        return a
    except Exception as e:
        logging.error('Error {} occured while getting data from shelve by key {}'.format(e, shelve_name))
        return None


def get_country_data(shelve_name, country):
    '''
    By the name of the storage shelve_name and country ID key
    receives data for one country for the entire period
    '''
    try:
        db = shelve.open(shelve_name)
        a = db[country]
        db.close()
        return a
    except Exception as e:
        logging.error('Error {} occured while getting data from shelve by key {}'.format(e, country))
        return None


def get_country_year_list(shelve_name, country):
    '''
    Retrieves year ID list by country ID
    for which there is data on the country
    '''
    try:
        db = shelve.open(shelve_name)
        a = db[country]
        a = list(a.keys())
        db.close()
        return a
    except Exception as e:
        logging.error('Error {} occured while getting country year list from shelve by key {}'.format(e, country))
        return None
    

def get_data_by_country_year(shelve_name, country, year):
    '''
    By country ID and year ID
    obtains data for a given country and
    given year
    '''
    try:
        db = shelve.open(shelve_name)
        a = db[country]
        a = a[year]
        db.close()
        return a
    except Exception as e:
        logging.error('Error {} occured while getting country {} data by year {} list from shelve by key {}'.format(e, country, year, shelve_name))
        return None
    

def update_by_country_year(shelve_name, country, year, new_data):
    '''
    Replaces data by country ID and year ID
    Data must be in the prescribed format.
    '''
    try:
        db = shelve.open(shelve_name)
        a = db[country]
        a[year] = new_data
        db[country] = a
        db.close()
        return 1
    except Exception as e:
        logging.error('Error {} occured while updating country {} data by year {} list from shelve by key {}'.format(e, country, year, shelve_name))
        return None


def delete_country_data(shelve_name, country):
    '''
    Removes data by country ID
    '''
    try:
        db = shelve.open(shelve_name)
        del db[country]
        db.close()
        return 1
    except Exception as e:
        logging.error('Error {} occured while deleting country data {} by key {}'.format(e, country, shelve_name))
        return None


def delete_country_year_data(shelve_name, country, year):
    '''
    Removes data by country ID and year ID
    '''
    try:
        db = shelve.open(shelve_name)
        a = db[country]
        b = dict(a)
        if year in b:
            del b[year]
        db[country] = b
        db.close()
        return 1
    except Exception as e:
        logging.error('Error {} occured while deleting country data {} by year {}'.format(e, country, year))
        return None


if __name__ == '__main__':
    
    country_list = get_country_list(SHELVE_NAME)
    one_country_data = get_country_data(SHELVE_NAME, country_list[0])
    one_country_year_list = get_country_year_list(SHELVE_NAME, country_list[0])
    one_country_one_year_data = get_data_by_country_year(SHELVE_NAME, country_list[0], one_country_year_list[7])
    