import os

import psycopg
from dotenv import load_dotenv
load_dotenv()
import plotly.graph_objects as go
import plotly.io as pio
pio.templates.default = 'plotly_dark'


# each author needs at least 50 user ratings, and each genre needs at least
# 50 authors in it. Further, we'll group by the first genre in the "top_genres"
# array, rather than try to do multiple runs of this query. top_genres[1] seems like
# a decent proxy for an author's most relevant genre.
AUTHOR_GENRE_GENDER_SPLIT_QUERY = '''
                                    SELECT 
                                        -- genre
                                        lower(top_genres[1]), 
                                        -- male share
                                        SUM(CASE WHEN g_comp = 'M' THEN 1 ELSE 0 END) * 1.0 / 
                                        (SUM(CASE WHEN g_comp = 'M' THEN 1 ELSE 0 END) + 
                                        SUM(CASE WHEN g_comp = 'F' THEN 1 ELSE 0 END)) AS share_men,
                                        -- sample size
                                        SUM(CASE WHEN g_comp = 'M' OR g_comp = 'F' THEN 1 ELSE 0 END) AS sample_size
                                    FROM
                                        pound
                                    INNER JOIN
                                        g_pound ON pound.author_id = g_pound.author_id
                                    WHERE 
                                        -- more confident gender prediction
                                        descr IS NOT NULL
                                    AND
                                        -- individual author has at least 50 ratings from Goodreads users
                                        rating_count >= 50
                                    AND
                                        top_genres[1] IS NOT NULL
                                    GROUP BY
                                        lower(top_genres[1])
                                    HAVING
                                        -- at least 50 authors in sample
                                        SUM(CASE WHEN g_comp = 'M' OR g_comp = 'F' THEN 1 ELSE 0 END) >= 50                                    
                                    ORDER BY
                                        3 DESC
                                  '''


# for users, also have the cutoff be at at least 50 users in a genre
USER_GENRE_GENDER_SPLIT_QUERY = '''
                                    WITH unnested_users(genre, g_nxg) AS (
                                        SELECT
                                            unnest(favorite_genres),
                                            g_nxg
                                        FROM
                                            false_dmitry
                                        INNER JOIN
                                            g_dmitry ON false_dmitry.user_id = g_dmitry.user_id
                                        WHERE
                                            (g_nxg = 'M' OR g_nxg = 'F')
                                        AND
                                            favorite_genres[1] IS NOT NULL
                                    )
                                    SELECT
                                        -- genre
                                        lower(genre),
                                        -- user male share
                                        CASE 
                                            WHEN SUM(CASE WHEN g_nxg = 'M' OR g_nxg = 'F' THEN 1 ELSE 0 END) > 0
                                            THEN (
                                                SUM(CASE WHEN g_nxg = 'M' THEN 1 ELSE 0 END) * 1.0 / 
                                                (SUM(CASE WHEN g_nxg = 'M' THEN 1 ELSE 0 END) + 
                                                SUM(CASE WHEN g_nxg = 'F' THEN 1 ELSE 0 END)) 
                                            ) 
                                            ELSE NULL
                                            END AS share_men,
                                        -- user sample size
                                        SUM(CASE WHEN g_nxg = 'M' OR g_nxg = 'F' THEN 1 ELSE 0 END) AS sample_size
                                    FROM
                                        unnested_users
                                    GROUP BY
                                        lower(genre)
                                    HAVING
                                        SUM(CASE WHEN g_nxg = 'M' OR g_nxg = 'F' THEN 1 ELSE 0 END) >= 50
                                '''


# given that there may be more female than male users, we'll need a baseline 
# to compare our results to.
USER_GENDER_SPLIT_BASELINE_QUERY = '''
                                    SELECT
                                        -- user male share
                                        SUM(CASE WHEN g_nxg = 'M' THEN 1 ELSE 0 END) * 1.0 / 
                                        (SUM(CASE WHEN g_nxg = 'M' THEN 1 ELSE 0 END) + 
                                        SUM(CASE WHEN g_nxg = 'F' THEN 1 ELSE 0 END)) AS share_men_overall,
                                        -- user sample size
                                        SUM(CASE WHEN g_nxg = 'M' OR g_nxg = 'F' THEN 1 ELSE 0 END) AS sample_size
                                    FROM
                                        false_dmitry
                                    INNER JOIN
                                        g_dmitry ON false_dmitry.user_id = g_dmitry.user_id
                                    WHERE
                                        favorite_genres[1] IS NOT NULL
                             '''


# also get baseline for authors
AUTHOR_GENDER_SPLIT_BASELINE_QUERY = '''
                                        SELECT 
                                            -- male share
                                            SUM(CASE WHEN g_comp = 'M' THEN 1 ELSE 0 END) * 1.0 / 
                                            (SUM(CASE WHEN g_comp = 'M' THEN 1 ELSE 0 END) + 
                                            SUM(CASE WHEN g_comp = 'F' THEN 1 ELSE 0 END)) AS share_men,
                                            -- sample size
                                            SUM(CASE WHEN g_comp = 'M' OR g_comp = 'F' THEN 1 ELSE 0 END) AS sample_size
                                        FROM
                                            pound
                                        INNER JOIN
                                            g_pound ON pound.author_id = g_pound.author_id
                                        WHERE 
                                            -- more confident gender prediction
                                            descr IS NOT NULL
                                        AND
                                            -- individual author has at least 50 ratings from Goodreads users
                                            rating_count >= 50
                                        AND
                                            top_genres[1] IS NOT NULL
                                     '''


def main():
    author_splits = []
    user_splits = []
    baseline_user_split = baseline_user_size = None
    baseline_author_split = baseline_author_size = None

    # GET DATA
    PG_STRING = os.getenv('PG_STRING')
    with psycopg.connect(conninfo=PG_STRING,
                         autocommit=True) as conn:
        with conn.cursor() as cur:
            # get author splits
            cur.execute(AUTHOR_GENRE_GENDER_SPLIT_QUERY)
            author_splits = [
                {
                    'genre': r[0].title(),
                    'share_men': r[1] * 100,
                    'share_women': 100 - (r[1] * 100),
                    'sample_size': r[2]
                } for r in cur.fetchall()
            ]
            
            # get user splits
            # only get the genres already included in the author splits
            cur.execute(USER_GENRE_GENDER_SPLIT_QUERY)
            user_splits = [
                {
                    'genre': r[0].title(),
                    'share_men': r[1] * 100,
                    'share_women': 100 - (r[1] * 100),
                    'sample_size': r[2]
                } for r in cur.fetchall() if r[0] in [r['genre'] for r in author_splits]
            ]
            
            # get user split baseling
            cur.execute(USER_GENDER_SPLIT_BASELINE_QUERY)
            r = cur.fetchone()
            baseline_user_split, baseline_user_size = r[0] * 100, r[1]

            # get author split baseling
            cur.execute(AUTHOR_GENDER_SPLIT_BASELINE_QUERY)
            r = cur.fetchone()
            baseline_author_split, baseline_author_size = r[0] * 100, r[1]
    
    # GET DATA INTO FIGURES
    # AUTHORS
    a_g = []
    a_m = []
    a_txt = []
    for d in author_splits:
        g = d['genre']
        g_share = round(d['share_men'], 2)
        g_size = d['sample_size']
        a_g.append(g)
        a_m.append(g_share)
        a_txt.append(f'<b>{g}</b><br>Male Share: {g_share}%<br>Sample Size: {g_size}')
    a_g.append('<b>GOODREADS POP. SPLIT</b>')
    a_m.append(baseline_author_split)
    a_txt.append(f'<b>GOODREADS POP. SPLIT</b><br>Male Share: {round(baseline_author_split, 2)}%<br>Sample Size: {baseline_author_size}')

    # GENERATE FIGURES
    fig = go.Figure(data=go.Bar(
        orientation='h',
        x=a_m[:20],
        y=a_g[:20],
        hovertext=a_txt[:20],
        hovertemplate='%{hovertext}<extra></extra>',
        marker={
            'color': '#C5EBDC',
            'opacity': 0.75
        }
    ))
    fig.update_xaxes(title_text = 'Goodreads Author Male Share', 
                     range=[0,100],
                     ticksuffix='%')
    fig.update_yaxes(title_text = 'Goodreads Author Genre')
    fig.write_html(file='out.html')
    return
    
    # USERS
    u_g = []
    u_m = []
    u_txt = []
    for g, g_dat in user_splits.items():
        u_g.append(g)
        u_m.append(g_dat['share_men'])
        u_txt.append(f'Male Share: {g_dat['share_men']}%<br>Sample Size: {g_dat['sample_size']}')
    u_g.append('<b>GOODREADS POP. SPLIT</b>')
    u_m.append(baseline_user_split)

    
    

if __name__ == '__main__':
    main()