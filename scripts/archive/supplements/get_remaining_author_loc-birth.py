'''
This script pulls data from WikiData, specifically on an author's
- birth date
- death date
- place of birth

Data pulled from here will be used to fill in missing birth/date/birthplace values
for authors in the "pound" table, primarily for the oldest authors.
'''

import os
import time
from typing import List, Tuple

import requests
import psycopg
from dotenv import load_dotenv
load_dotenv()


INSERT_STATEMENT = '''
                    INSERT INTO
                        wikidata_lb (
                            author_code,
                            author_lab,
                            native_name,
                            occupation_lab,
                            dob,
                            dod,
                            pob,
                            pob_lab
                        )
                    VALUES (
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    '''

SPARQL_QUERY = '''
                SELECT ?author ?authorLabel ?nativeName 
                        ?occupation ?occupationLabel 
                        ?dob ?dod 
                        ?pob ?pobLabel
                WHERE {
                    VALUES ?occupation {
                        wd:Q482980 # author
                        wd:Q36180 # writer
                        wd:Q4964182 # philosopher
                        wd:Q1234713 # theologian
                        wd:Q49757 # poet
                        wd:Q201788 # historian
                        wd:Q333634 # translator
                        wd:Q214917 # playwright
                        wd:Q361809 # rhetorician
                        wd:Q6625963 # novelist
                        wd:Q901 # scientist
                        wd:Q170790 # mathematician
                        wd:Q16314501 # encyclopedist
                        wd:Q12859263 # orator
                        wd:Q82955 # politician
                        wd:Q864380 # biographer
                        wd:Q15980158 # nonfiction writer
                    }
                    ?author wdt:P106 ?occupation. # must be one of the occupations listed above

                    OPTIONAL { ?author wdt:P569 ?dob. }
                    OPTIONAL { ?author wdt:P570 ?dod. }
                    OPTIONAL { ?author wdt:P19 ?pob. }
                    OPTIONAL { ?author wdt:P1559 ?nativeName. } 

                    %s

                    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
                }
                '''


def get_birth_loc(session: requests.Session,
                        filter_query_str: str) -> List[Tuple[str]]:
    # send reqs for author birth/death date and location
    URL_BASE = 'https://query.wikidata.org/sparql'
    q_param = SPARQL_QUERY % filter_query_str
    PARAMS = {
        'query': q_param,
        'format': 'json'
    }
    HEADERS = {
        'User-Agent': 'ancient-authors-puller',
        'Accept': 'application/sparql-results+json'
    }
    with session.get(url=URL_BASE, 
                    params=PARAMS,
                    headers=HEADERS) as resp:
        res = resp.json()
    dat = res['results']['bindings']

    # COLUMNS(POSITION): author(0), author_lab(1), native_name(2), occ_lab(3), dob(4), dod(5), pob(6), pob_lab(7)
    authors: List[Tuple[str]] = []
    for author in dat:
        # fix BC dates of birth/death
        dob = author.get('dob', {}).get('value', None)
        dod = author.get('dod', {}).get('value', None)
        if dob:
            if dob.startswith('http'): # some contain links to broken pages
                dob = None
            else:
                if dob.startswith('-'):
                    dob = dob[1:] + ' BC'
                dob = '0001' + dob[4:] if dob.startswith('0000') else dob # handle year 0 issue with postgres date type
        if dod:
            if dod.startswith('http'):
                dod = None
            else:
                if dod.startswith('-'):
                    dod = dod[1:] + ' BC'
                dod = '0001' + dod[4:] if dod.startswith('0000') else dod

        authors.append(
            (
                author['author']['value'],
                author['authorLabel']['value'],
                author.get('nativeName', {}).get('value', None),
                author['occupationLabel']['value'],
                dob,
                dod,
                author.get('pob', {}).get('value', None),
                author.get('pobLabel', {}).get('value', None)
            )
        )
    return authors


def main():
    # get data on birth place and birth/death date for authors born/dead before 1900
    # send SPARQL request to WikiData
    FILTER_GRPS = [
        # born/died before 1000
        'FILTER(?dob <= "1000-01-01"^^xsd:dateTime || ?dod <= "1000-01-01"^^xsd:dateTime).'
    ]
    # get the remaining groups
    for c in range(9):
        fg = f'FILTER((?dob >= "1{c}01-01-01"^^xsd:dateTime || ?dod >= "1{c}01-01-01"^^xsd:dateTime) && ' \
             f'(?dob <= "1{c+1}00-01-01"^^xsd:dateTime || ?dod <= "1{c+1}00-01-01"^^xsd:dateTime)).'
        FILTER_GRPS.append(fg)

    PG_STRING = os.getenv("PG_STRING")
    with psycopg.connect(conninfo=PG_STRING,
                         autocommit=True) as conn:
        with conn.cursor() as cur:
            with requests.Session() as sesh:
                for fg in FILTER_GRPS:
                    print(f'QUERY: {fg}')
                    author_data = get_birth_loc(session=sesh, filter_query_str=fg)
                    print(f'RECIEVED LEN: {len(author_data)}')
                    cur.executemany(INSERT_STATEMENT, author_data)
                    time.sleep(60) # minute delay in between each req, just to be safe


if __name__ == '__main__':
    main()