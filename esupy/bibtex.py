# bibtex.py (esupy)
# !/usr/bin/env python3
# coding=utf-8

"""
Module to support generating sources within the olca_schema.
"""

from pathlib import Path
import logging as log

from esupy.util import make_uuid

def generate_sources(bib_path: Path,
                     bibids: dict
                     ) -> list:
    """
    Generates a list of olca_schema.Source based on requested bib_ids.
    :param bib_path: Path object to a .bib file containing source information
    :param bibids: dictionary in the format of {Name: bib_id}, where name is the
        displayed in the openLCA dashboard.
    :return: list of olca_schema.Source
    """
    try:
        import bibtexparser
        from bibtexparser.bparser import BibTexParser
    except ImportError:
        log.warning("Writing sources requires bibtexparser package")
        return []
    try:
        import olca_schema as o
    except ImportError:
        log.warning("Writing sources requires olca_schema package")
        return []

    def customizations(record):
        """Use some functions delivered by the library

        :param record: a record
        :returns: -- customized record
        """
        #record = bibtexparser.customization.author(record)
        record = bibtexparser.customization.add_plaintext_fields(record)
        record = bibtexparser.customization.doi(record)

        return record

    parser = BibTexParser(common_strings=True)
    parser.ignore_nonstandard_types = False
    parser.homogenize_fields = True
    parser.customization = customizations

    def read_bib_file(path: str):
        with open(path) as bibtex_file:
            bib_database = parser.parse_file(bibtex_file)

        return bib_database.entries_dict


    def parse_for_olca(bibids, d):

        key_dict = {'description': ['plain_author',
                                    'plain_publisher',
                                    'plain_title',
                                    'plain_journal',
                                    'year'],
                    'textReference': '',
                    'year': 'plain_year',
                    'url': 'url',
                    }
        s = []
        for bibid, name in bibids.items():
            try:
                record = d[bibid]
            except KeyError:
                print(f'{bibid} not found')
                continue
            source = {}
            source['name'] = bibids[bibid]
            for key, value in key_dict.items():
                try:
                    if isinstance(value, list):
                        source[key] = ', '.join([record[v] for v in value if v in record])
                    else:
                        source[key] = record[value]
                except KeyError:
                    source[key] = ''
            source['@id'] = make_uuid(source['description'])
            s.append(o.Source.from_dict(source))
        return s

    d = read_bib_file(bib_path)
    source_list = parse_for_olca(bibids, d)
    return source_list

if __name__ == "__main__":
    source_list = generate_sources(
        bib_path = Path(__file__).parents[1] / 'tests' / 'test.bib',
        bibids = {'bare_traci_2011': 'TRACI 2.1'})
