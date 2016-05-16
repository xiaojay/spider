#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- author: jat@wegene.com -*-
# -*- requirements: sqlalchemy, mysqlclient -*-

import re

from sqlalchemy import Column, create_engine
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
    name = Column(VARCHAR(64), index=True, nullable=False, unique=True)


class GeneSnp(Base):
    rsid = Column(VARCHAR(32), index=True, nullable=False, unique=True)
    chromosome = Column(VARCHAR(2), index=True, nullable=False)
    position = Column(INTEGER(unsigned=True), index=True, nullable=False)


class GeneSnpIndex(Base):
    gene_id = Column(INTEGER(unsigned=True), index=True, nullable=False)
    snp_id = Column(INTEGER(unsigned=True), index=True, nullable=False)


engine = create_engine('mysql+mysqldb://user:password@host/dbname', connect_args={'charset': 'utf8'}, encoding='utf-8', echo=True, pool_recycle=3600)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
s = sessionmaker(engine)()

with open('ann4.txt', 'r', encoding='utf-8') as f:
    for l in f:
        l = l.split('\t')

        gene = s.query(Gene).filter_by(name=l[3]).first()
        if gene is None:
            gene = Gene(name=l[3])
            s.add(gene)

        snp = s.query(GeneSnp).filter_by(rsid=l[0]).first()
        if snp is None:
            snp = GeneSnp(rsid=l[0], chromosome=l[1], position=l[2])
            s.add(snp)

        s.commit()

        s.add(GeneSnpIndex(gene_id=gene.id, snp_id=snp.id))
        s.commit()
