#!/usr/bin/env python

import re

import requests


s = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=100)
b = requests.adapters.HTTPAdapter(max_retries=100)
s.mount('http://', a)
s.mount('https://', b)

s.headers.update({'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2618.8 Safari/537.36'})

r = s.get('https://www.23andme.com/user/signin/')

key = re.search(r'<input type="hidden" name="__context__" value="([^"]+)" />', r.text).group(1)

s.post('https://www.23andme.com/cas/signin/', data={
    'username': 'example@example.com',
    'password': 'example',
    '__source_node__': 'start',
    '__context__': key,
    '__form__': 'login',
})

r = s.get('https://you.23andme.com/reports/?category=carrier_status')

m = re.findall(r'<a.+?href="([^"]+)".+?data-category="carrier_status".+?data-title="([^"]+)" >', r.text)

data = {}
i = 0
for v in m:
    data[i] = {}

    data[i]['name'] = v[1]

    data[i]['scientific_details'] = 'https://you.23andme.com' + v[0] + 'details/'

    while True:
        try:
            r = s.get(data[i]['scientific_details'])
        except Exception:
            continue
        else:
            break

    data[i]['gene'] = re.search(r'data-gene="([^"]+)"', r.text).group(1)

    data[i]['markers'] = {}
    n = 0

    marker = re.findall(r'data-marker="([^"]+)"', r.text)
    for val in marker:
        data[i]['markers'][n] = {}
        data[i]['markers'][n]['marker'] = val
        n += 1

    n = 0
    hgvs = re.findall(r'<h4 class="variant-mobile-header">([^<]+(?:<sup>[^<]+</sup>[^<]+)?)</h4>', r.text)
    for val in hgvs:
        data[i]['markers'][n]['hgvs'] = val.replace('<sup>', '').replace('</sup>', '').strip()
        n += 1

    clinvar = re.findall(r'http://www\.ncbi\.nlm\.nih\.gov/clinvar/variation/\d+/', r.text)
    if clinvar:
        n = 0
        for val in clinvar:
            data[i]['markers'][n]['clinvar'] = val

            while True:
                try:
                    r = requests.get(data[i]['markers'][n]['clinvar'], timeout=120)
                except Exception:
                    continue
                else:
                    break

            if r.status_code == 200:
                data[i]['markers'][n]['rsid'] = re.search(r'<a href="/variation/tools/1000genomes/[^"]+">([^<]+)</a>', r.text).group(1)
                data[i]['markers'][n]['position'] = re.search(r'<span class="ddulregtext">(Chr[^\(]+)\(on Assembly GRCh37\)</span>', r.text).group(1).strip()
            else:
                data[i]['markers'][n]['rsid'] = ''
                data[i]['markers'][n]['position'] = ''

            n += 1
    else:
        for k in data[i]['markers']:
            data[i]['markers'][k]['clinvar'] = ''
            data[i]['markers'][k]['rsid'] = ''
            data[i]['markers'][k]['position'] = ''

    i += 1

with open('23andme.Carrier.Status.txt', 'w') as f:
    for k, v in data.iteritems():
        for key, val in v['markers'].iteritems():
            l = '\t'.join([v['name'], v['gene'], val['marker'], val['hgvs'], val['rsid'], val['position'], v['scientific_details'], val['clinvar']]) + '\n'
            f.write(l.encode('utf-8'))
