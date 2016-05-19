#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- author: jat@wegene.com -*-
# -*- requirements: requests, sqlalchemy, mysqlclient -*-

import re
import json
import io

import requests
from sqlalchemy import Column, event, create_engine
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy.orm import sessionmaker


@as_declarative()
class Base(object):
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
        'mysql_collate': 'utf8_unicode_ci',
    }

    id = Column(INTEGER(unsigned=True), primary_key=True)

    @declared_attr
    def __tablename__(cls):
        return re.sub('(?!^)([A-Z]+)', r'_\1', cls.__name__).lower()


class Gene(Base):
    approved_symbol = Column(VARCHAR(64), index=True, nullable=False)
    approved_name = Column(VARCHAR(256), index=True, nullable=False)
    chromosome = Column(VARCHAR(64), index=True, nullable=False, server_default='')
    hgnc_id = Column(VARCHAR(32), nullable=False, server_default='')
    accession_numbers = Column(VARCHAR(512), nullable=False, server_default='[]')
    ensembl_gene_id = Column(VARCHAR(32), nullable=False, server_default='')
    specialist_database_links = Column(VARCHAR(1024), nullable=False, server_default='{}')
    pubmed_ids = Column(VARCHAR(512), nullable=False, server_default='[]')
    ccds_ids = Column(VARCHAR(512), nullable=False, server_default='[]')
    vega_id = Column(VARCHAR(32), nullable=False, server_default='')
    entrez_gene_id = Column(VARCHAR(32), nullable=False, server_default='')
    omim_id = Column(VARCHAR(32), nullable=False, server_default='')
    refseq = Column(VARCHAR(32), nullable=False, server_default='')
    uniprot_id = Column(VARCHAR(32), nullable=False, server_default='')
    ucsc_id = Column(VARCHAR(32), nullable=False, server_default='')
    status = Column(VARCHAR(32), nullable=False)


class GenePreviousSymbols(Base):
    name_id = Column(INTEGER(unsigned=True), index=True, nullable=False)
    previous_symbol = Column(VARCHAR(64), index=True, nullable=False)


class GeneSynonyms(Base):
    name_id = Column(INTEGER(unsigned=True), index=True, nullable=False)
    synonym = Column(VARCHAR(64), index=True, nullable=False)


def json_dumps(mapper, connection, target):
    for i in ('accession_numbers', 'pubmed_ids', 'ccds_ids'):
        setattr(target, i, json.dumps(getattr(target, i)))

event.listen(Gene, 'before_insert', json_dumps)

r = requests.get('http://www.genenames.org/cgi-bin/download?col=gd_app_sym&col=gd_app_name&col=gd_prev_sym&col=gd_aliases&col=gd_pub_chrom_map&col=gd_hgnc_id&col=gd_pub_acc_ids&col=gd_pub_ensembl_id&col=gd_other_ids&col=gd_pubmed_ids&col=gd_ccds_ids&col=md_vega_id&col=gd_pub_eg_id&col=md_mim_id&col=md_refseq_id&col=md_prot_id&col=md_ucsc_id&col=gd_status&status=Approved&status=Entry+Withdrawn&status_opt=2&where=&order_by=gd_app_sym_sort&format=text&limit=&hgnc_dbtag=on&submit=submit')
f = io.StringIO(r.text)
# f = open('hgnc.csv', 'r', encoding='utf-16')

engine = create_engine('mysql+mysqldb://user:password@host/dbname', connect_args={'charset': 'utf8'}, encoding='utf-8', echo=True, pool_recycle=3600)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
s = sessionmaker(engine)()

# name = ['approved_symbol', 'approved_name', 'previous_symbols', 'synonyms', 'chromosome', 'hgnc_id', 'accession_numbers', 'ensembl_gene_id', 'specialist_database_links', 'pubmed_ids', 'ccds_ids', 'vega_id', 'entrez_gene_id', 'omim_id', 'refseq', 'uniprot_id', 'ucsc_id', 'status']
name = [i.split('(')[0].strip().replace(' ', '_').lower() for i in f.readline().split('\t')]
multiples = ['previous_symbols', 'synonyms', 'accession_numbers', 'pubmed_ids', 'ccds_ids']

for l in f.readlines():
    l = l.split('\t')

    data = {}
    for k, v in enumerate(l):
        if name[k] in multiples:
            if v.strip():
                v = [i.strip() for i in v.split(',')]
            else:
                v = []
        elif name[k] == 'specialist_database_links':
            m = re.findall(r'<a href="([^"]+)">([^<]+)</a>', v)

            v = {}
            if m:
                for i in m:
                    v[i[1]] = i[0]
            v = json.dumps(v)
        else:
            v = v.strip()

        data[name[k]] = v

    previous_symbols = data.pop('previous_symbols')
    synonyms = data.pop('synonyms')

    gene_name = Gene(**data)
    s.add(gene_name)
    s.commit()

    if previous_symbols:
        for i in previous_symbols:
            s.add(GenePreviousSymbols(name_id=gene_name.id, previous_symbol=i))
    if synonyms:
        for i in synonyms:
            s.add(GeneSynonyms(name_id=gene_name.id, synonym=i))
    s.commit()

f.close()
