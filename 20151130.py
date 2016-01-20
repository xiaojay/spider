#! /usr/bin/env/python
# -*- coding: utf-8 -*-

import logging
import sys
import os
import json
import re
import time

import MySQLdb
import requests


dir = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.join(dir, 'log.txt'),
    filemode='w',
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

db = MySQLdb.connect(user='root', passwd='passwd', db='genedata', use_unicode=True, charset='utf8')

c = db.cursor(MySQLdb.cursors.DictCursor)

c.execute(u'select cases.caseid, cases.description, cases.description_en, cases.category, snps.snpid, snps.rsid, cases.caseid, case_snps.snpid from cases, snps, case_snps where cases.description_en <> "" and cases.description_en is not null and cases.category = "健康风险" and cases.enable = 1 and cases.caseid = case_snps.caseid and case_snps.snpid = snps.snpid')

row = c.fetchone()

if row is None:
    logging.error('No data.')
    sys.exit(1)

with open(os.path.join(dir, 'if_2015.json')) as f:
    if_2015 = json.load(f)

f = open(os.path.join(dir, 'result.txt'), 'w')


def get(url, method='GET', data={}, r=None):
    if r is None:
        r = requests.get('http://www.ncbi.nlm.nih.gov/pubmed/')

    args = {
        'method': method,
        'url': url,
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.10 Safari/537.36',
        },
        'cookies': r.cookies,
        'timeout': 60,
    }

    if data:
        args['data'] = data

    time.sleep(5)

    try:
        _r = requests.request(**args)
    except Exception as e:
        logging.error('Request failed, try again after 20 seconds. %s' % e)
        time.sleep(20)

        return get(url, method, data, r)
    else:
        return _r


r = None
while row is not None:
    logging.debug('Row: %s' % row)

    r = get('http://www.ncbi.nlm.nih.gov/pubmed/?term=%s' % row['rsid'], r=r)
    n = 1

    name = '%s_%s' % (row['description'], row['rsid'])
    data = []
    while True:
        logging.info('Searching %s on ncbi. Page %d.' % (name, n))
        logging.debug('Cookies: %s' % dict(r.cookies))

        key = re.search(r'<input name="EntrezSystem2\.PEntrez\.DbConnector\.LastQueryKey" sid="1" type="hidden" value="(\d+)" />', r.text).group(1)
        logging.debug('Last query key: %s' % key)

        r = get('http://www.ncbi.nlm.nih.gov/pubmed/', 'POST', {
            'term': row['rsid'],
            'EntrezSystem2.PEntrez.DbConnector.Cmd': 'PageChanged',
            'EntrezSystem2.PEntrez.PubMed.Pubmed_ResultsPanel.Pubmed_DisplayBar.PageSize': 100,
            'EntrezSystem2.PEntrez.PubMed.Pubmed_ResultsPanel.Entrez_Pager.CurrPage': n,
            'EntrezSystem2.PEntrez.DbConnector.LastQueryKey': key,
        }, r)
        # logging.debug('HTML: %s' % r.text.replace('\n', ' '))

        pubmed = re.findall(r'<a href="(/pubmed/\d+)"', r.text)

        if not pubmed:
            logging.info('Search %s Done.' % name)
            break

        for m in pubmed:
            logging.debug('Found %s.' % m)

            r = get('http://www.ncbi.nlm.nih.gov%s' % m, r=r)
            # logging.debug('HTML: %s' % r.text.replace('\n', ' '))

            if not re.search(row['description_en'], r.text):
                logging.info('Can not found %s, ignore.' % row['description_en'])

            jour_date = re.search(r'<a href="#" title=".+?" abstractLink="yes" alsec="jour" alterm=".+?">(.+?)\.</a> (.+?);', r.text)

            if jour_date.group(1).upper() not in if_2015:
                logging.info('Jour %s not in if_2015, ignore.' % jour_date.group(1))
                continue

            new = {
                'name': row['description'],
                'name_en': row['description_en'],
                'snp': row['rsid'],
                'jour': jour_date.group(1),
                'date': jour_date.group(2),
                'if': if_2015[jour_date.group(1).upper()],
                'title': re.search(r'<h1>(.+?)</h1>', r.text).group(1),
                'link': 'http://www.ncbi.nlm.nih.gov%s' % m,
            }

            logging.debug('New data: %s.' % new)

            if len(data) < 10:
                data.append(new)
            else:
                if new['if'] <= data[-1]['if']:
                    logging.info('The if less than or equal to last jour\'s if (%f), ignore.' % data[-1]['if'])
                else:
                    data[-1] = new

            data.sort(key=lambda i: i['if'], reverse=True)

        n += 1

    logging.debug('%s: %s' % (name, data))
    for v in data:
        f.write(u'{name}\t{name_en}\t{snp}\t{jour}\t{date}\t{if}\t{title}\t{link}\n'.format(**v).encode('utf8'))
    f.flush()

    row = c.fetchone()

db.close()
f.close()
