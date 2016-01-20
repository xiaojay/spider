#! /usr/bin/env/python
# -*- coding: utf-8 -*-

import logging
import os
import re
import time

import requests


dir = os.path.dirname(os.path.abspath(__file__))

# do some tricks for write log to file with encoding utf-8
handler = logging.FileHandler(os.path.join(dir, 'log.txt'), 'w', 'utf-8')
handler.setFormatter(logging.Formatter('%(levelname)s %(asctime)s %(name)s %(message)s', '%Y-%m-%d %H:%M:%S'))
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# py2: f = open(os.path.join(dir, 'result.txt'), 'w')
f = open(os.path.join(dir, 'result.txt'), 'w', encoding='utf-8')

s = requests.Session()


def get(url):
    args = {
        'url': url,
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.10 Safari/537.36',
        },
        'timeout': 60,
        'allow_redirects': True,
    }

    time.sleep(1)

    try:
        r = s.get(**args)
    except Exception as e:
        logger.error('Request failed, try again after 10 seconds. %s' % e)
        time.sleep(10)

        return get(url)
    else:
        # requests use ISO-8859-1 by default if server doesn't send encoding.
        r.encoding = r.apparent_encoding

        logger.debug('Request headers: %s' % r.request.headers)
        logger.debug('Response headers: %s' % r.headers)
        # I do no know why r.text.replace('\r\n', ' ') not work.
        logger.debug('HTML: ' + ' '.join(r.text.splitlines()))

        m = re.search(r'self\.location="(.+?)"', r.text)

        if m is not None:
            logger.info('Safe Dog enabled, redirect to %s' % m.group(1))

            return get('http://www.hanjianbing.org' + m.group(1))

        return r


r = get('http://www.hanjianbing.org/database/')

m = re.findall(r'<dt><a href="(.+?)" title=".+?">(.+?)</a><span>(.+?)</span></dt>', r.text)

for v in m:
    # py2: f.write((u'%s\n%s\n\n\n' % (v[1], v[2])).encode('utf-8'))
    f.write(u'%s\n%s\n\n\n' % (v[1], v[2]))

    r = get(v[0])

    while True:
        mat = re.findall(r'<li><a href="(.+?)" title=".+?" target="_blank">.+?</a></li>', r.text)

        for val in mat:
            rq = get(val)

            match = re.findall(r'<li><span>(.+?：)</span>(.+?)</li>', rq.text)

            for value in match:
                f.write(value[0] + value[1] + '\n')

            f.write('\n')

            match = re.search(r'<!--right_cont begin-->.+?<p.+?>(.+)<div class="set_time">', rq.text, re.DOTALL)
            match = re.sub(r' *<.+?>(?:&nbsp;| )*', '', match.group(1))

            for line in match.splitlines():
                line = line.strip()

                if not line or line == '&nbsp;':
                    continue

                f.write(line + '\n')

            f.write('\n\n')

        mat = re.search('<a class="on"  title="\d+">\d+</a>  <a href="(.+?)" class="" title="\d+">\d+</a>', r.text)
        if mat is None:
            break

        r = get(mat.group(1))

    f.write('----------------------------------------\n\n\n')
    f.flush()

f.close()
