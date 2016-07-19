#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The MIT License (MIT)
Copyright (c) 2016 Thomas Lin

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"),to deal in the Software
without restriction, including without limitation the rights to use, copy, modify, 
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""

import requests
from pyquery import PyQuery
import re
import csv
import datetime
import logging

# Uncomment the following if multithread is used.
# For full documentation: https://docs.python.org/2/library/threading.html
# import threading


CTRIP_CHINA_LOCATION_BASE_URL = 'http://you.ctrip.com/countrysightlist/china110000'
CTRIP_BASE_URL = 'http://you.ctrip.com'
logging.basicConfig(filename='crawler.log', level=logging.DEBUG)


def get_ctrip_china_city_list(start, end):
    """
    Getting China city list from ctrip
    :param start: starting page number
    :type start: int
    :param end: ending page number
    :type end: int
    
    :return: list
    """
    cities = []
    for i in xrange(start, end + 1):
        url = '{}/p{}.html'.format(CTRIP_CHINA_LOCATION_BASE_URL, i)
        try:
            res = requests.get(url)
            raw = PyQuery(res.text)
            tmp = raw('.list_mod1 > dl').map(lambda i, el: {
                'city_name':    PyQuery(el)('dt > a').text(),
                'url':          '{}{}'.format(CTRIP_BASE_URL, PyQuery(PyQuery(el)('dd')[1])('a').attr('href'))
            })
            cities += tmp
        except requests.RequestException:
            log = '[{:%Y-%m-%d %H:%M:%S}] Error in getting city list info at page {}. Error: {}'.format(
                datetime.datetime.now(),
                i,
                requests.RequestException.message
            )
            logging.warning(log)
            print log

    return cities


def get_category(city):
    """
    Getting different categories of tourist traps of a given city
    :param city: dictionary consists of city name and associated url
    :type city: dict
    
    :return: list
    """
    url = city.get('url')
    categories = []
    try:
        logging.info('[{:%Y-%m-%d %H:%M:%S}] Retrieving category list for {}'.format(
            datetime.datetime.now(),
            city.get('city_name').encode('utf-8')
        ))
        res = requests.get(url)
        raw = PyQuery(res.text)
        tmp = raw('.search_wide > ul > li > dl > dd > a').map(lambda i, el: {
            'category': PyQuery(el).text(),
            'url':      '{}/s{}.html'.format(
                url.replace('.html', ''),
                re.search('[0-9]+', PyQuery(el).attr('onclick')).group(0)
            )
        })
        categories += tmp
        return categories
    except requests.RequestException:
        log = '[{:%Y-%m-%d %H:%M:%S}] Error in getting city info for {}. Error: {}'.format(
            datetime.datetime.now(),
            city.get('city_name').encode('utf-8'),
            requests.RequestException.message
        )
        logging.warning(log)
        print log


def get_category_location(category):
    """
    Getting sight info f given category
    :param category: dictionary consists of category name and associated url
    :type category: dict
    
    :return: list
    """
    logging.info('[{:%Y-%m-%d %H:%M:%S}] Starting getting data for category: {}...'.format(
        datetime.datetime.now(),
        category.get('category').encode('utf-8')
    ))
    url = category.get('url')
    res = requests.get(url)
    raw = PyQuery(res.text)
    page_num = int(raw('.numpage').text()) if raw('.numpage').text() is not '' else 1
    output = []

    for i in xrange(1, page_num + 1):
        print('[{:%Y-%m-%d %H:%M:%S}] Getting page {} data out of {}...'.format(
            datetime.datetime.now(),
            i,
            page_num
        ))
        try:
            _res = requests.get('{}-p{}.html'.format(url.replace('.html', ''), i))
            _raw = PyQuery(_res.text)
            tmp = _raw('.rdetailbox').map(lambda i, el: {
                'name':     PyQuery(el)('dl > dt > a').text(),
                'address':  PyQuery(el)('dl > dd.ellipsis').text(),
                'score':    PyQuery(el)('ul.r_comment > li > a.score > strong').text()
            })
            output += tmp
        except requests.RequestException:
            log = '[{:%Y-%m-%d %H:%M:%S}] Error in getting sight info. At category {} page {}. Error: {}'.format(
                datetime.datetime.now(),
                category.get('category').encode('utf-8'),
                i,
                requests.RequestException.message
            )
            logging.warning(log)
            print log
    return output


def run_crawler(start, end):
    """
    Starting the crawler task and generate output csv file
    :param start: starting page number
    :type start: int
    :param end: ending page number
    :type end: int
    """
    cities = get_ctrip_china_city_list(start=start, end=end)
    res = []
    for city in cities:
        logging.info('[{:%Y-%m-%d %H:%M:%S}] Starting getting data for {}...'.format(
            datetime.datetime.now(),
            city.get('city_name').encode('utf-8')
        ))

        categories = get_category(city)
        for c in categories:
            locations = get_category_location(c)
            for location in locations:
                res.append({
                    'city':     city.get('city_name').encode('utf-8'),
                    'category': c.get('category').encode('utf-8'),
                    'name':     location.get('name').encode('utf-8'),
                    'address':  location.get('address').encode('utf-8'),
                    'score':    location.get('score').encode('utf-8')
                })

        logging.info('[{:%Y-%m-%d %H:%M:%S}] Finishing getting data for {}.'.format(
            datetime.datetime.now(),
            city.get('city_name').encode('utf-8')
        ))

    keys = res[0].keys()
    with open('result__{:%Y-%m-%d %H_%M_%S}__{}-{}.csv'.format(datetime.datetime.now(), start, end), 'wb') as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(res)


if __name__ == "__main__":
    run_crawler(1, 10)
