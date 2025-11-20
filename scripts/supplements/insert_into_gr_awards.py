import os
import json

import psycopg
from dotenv import load_dotenv
load_dotenv()

def main():
    # insert goodreads book winnders into the gr_awards table
    GR_AWARDS_DIR = os.path.join('data', 'goodreads-choice-awards', 'books')
    GR_AWARDS_YR_RANGE = [yr for yr in range(2011,2025)]

    INSERT_STATEMENT = '''
                        INSERT INTO 
                            gr_awards(
                                book_id, 
                                author_id, 
                                award_year, 
                                award_category, 
                                award_num_votes
                            )
                        VALUES (
                            %s, %s, %s, %s, %s
                        )
                       '''

    PG_STRING = os.getenv('PG_STRING')
    with psycopg.connect(conninfo=PG_STRING,
                         autocommit=True) as conn:
        with conn.cursor() as cur:
            
            for yr in GR_AWARDS_YR_RANGE:
                insert_set = []

                dat_path = os.path.join(GR_AWARDS_DIR, f'winners_{yr}.json')
                with open(dat_path, 'r') as jf:
                    dat = json.load(jf)
                
                for bk in dat['results']:
                    bk_dat = (
                        bk['id'], 
                        bk['author_id'], 
                        bk['award_year'], 
                        bk['award_category'],
                        bk['award_num_votes']
                    )
                    insert_set.append(bk_dat)
                
                cur.executemany(INSERT_STATEMENT, insert_set)
                print(f'YEAR: {yr} - {len(insert_set)} BOOKS INSERTED')


if __name__ == '__main__':
    main()


                