import os
import re

import psycopg
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
pio.templates.default = 'plotly_dark'


GENRE_COLORS = {
    'Sci-Fi/Fantasy': "#daeaf6",
    'Philosophy/Politics/Economics': "#d7e3fc",
    'Nonfiction': "#e1dbd6",
    'Manga/Comics': "#ffb3c6",
    'Cooking': "#ddedea",
    'Music': "#eddcd2",
    'Humor': "#eac9c1",
    'Horror': "#c1d3fe",
    'Mystery/Crime': "#f08080",
    'Romance/Erotica': "#fcf5c7",
    'Young Adult': "#ffe5ec",
    'Children': "#84dcc6",
    'Queer': "#fce1e4",
    'Poetry': "#fbc4ab",
    'Fiction': "#ffa69e",
    'Horror/Suspence': "#fffbff",
    'Classics': "#ff686b",
    'History': "#d3ab9e",
    'Psychology/Sociology': "#b5d6d6",
    'Religion/Spirituality': "#eeddd3",
    'Sports': "#ffc09f",
    'Business': "#f0efeb",
    'STEM': "#fcf4dd",
    'Art/Photography': "#55d6c2",
    'Self Help': "#ffc2d1",
    'Biography/Memoir': "#fb6f92",
    'Contemporary': "#e8dff5",
    'Chick-Lit': "#c6e2e9"
}


def broad_genre(genre: str) -> str:
    # returns the broader genre group for a given genre
    # returns "Other" if genre not applicable
    # all patterns
    patterns = {
        r'sci(ence)?[-\s]fi(ction)|fantasy': 'Sci-Fi/Fantasy',
        r'philosop|poli(tic|cy)|econom': 'Philosophy/Politics/Economics',
        r'non-?fiction': 'Nonfiction',
        r'manga|comic|graphic[-\s]novel': 'Manga/Comics',
        r'food|cook|kitchen|recipe': 'Cooking',
        r'music': 'Music',
        r'humor|comedy': 'Humor',
        r'horror': 'Horror',
        r'myster|crim(e|inal)|thriller': 'Mystery/Crime',
        r'roman[tc]|erotic': 'Romance/Erotica',
        r'^y\.?a\.?$|young[\s-]adult': 'Young Adult',
        r'children': 'Children',
        r'gay|queer|lgbt|lesbian': 'Queer',
        r'poet': 'Poetry',
        r'^(?!science)[\s-]?fiction': 'Fiction',
        r'horror|suspense': 'Horror/Suspence',
        r'classics': 'Classics',
        r'history': 'History',
        r'psych|sociol': 'Psychology/Sociology',
        r'theology|christian|catholi|islam|judai|jewis|religio|spirit|pagan': 'Religion/Spirituality',
        r'sports|athlet': 'Sports',
        r'business|financ': 'Business',
        r'computer|tech|^science$|math': 'STEM',
        r'art|craft|photo|film': 'Art/Photography',
        r'self[-\s]?help': 'Self Help',
        r'biograph|memoir': 'Biography/Memoir',
        r'contemporary|modern': 'Contemporary',
        r'chick[\s-]lit': 'Chick-Lit'
    }
    for pat, lab in patterns.items():
        if re.search(pat, genre):
            return lab
    return 'Other'
        
    



# each author needs at least 100 user ratings, and each genre needs at least
# 100 authors in it.
AUTHOR_GENRE_GENDER_SPLIT_QUERY = '''   
                                    WITH unnested_authors(genre, g_comp) AS (
                                        SELECT
                                            unnest(top_genres),
                                            g_comp
                                        FROM
                                            pound
                                        INNER JOIN
                                            g_pound ON pound.author_id = g_pound.author_id
                                        WHERE
                                            (g_comp = 'M' OR g_comp = 'F')
                                        AND
                                            top_genres[1] IS NOT NULL
                                        AND
                                            rating_count >= 100
                                        AND
                                            descr IS NOT NULL
                                    )
                                    SELECT 
                                        -- genre
                                        replace(lower(genre), ' and ', ' & '),
                                        -- male count
                                        SUM(CASE WHEN g_comp = 'M' THEN 1 ELSE 0 END) * 1.0,
                                        -- female count
                                        SUM(CASE WHEN g_comp = 'F' THEN 1 ELSE 0 END) * 1.0
                                    FROM
                                        unnested_authors
                                    GROUP BY 
                                        replace(lower(genre), ' and ', ' & ')
                                    HAVING
                                        -- at least 100 authors in sample   
                                        SUM(CASE WHEN g_comp = 'M' OR g_comp = 'F' THEN 1 ELSE 0 END) >= 100
                                  '''


# for users, also have the cutoff be at at least 100 users in a genre
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
                                        replace(lower(genre), ' ', '-'),
                                        -- male count
                                        SUM(CASE WHEN g_nxg = 'M' THEN 1 ELSE 0 END),
                                        -- female count
                                        SUM(CASE WHEN g_nxg = 'F' THEN 1 ELSE 0 END)
                                    FROM
                                        unnested_users
                                    GROUP BY
                                        replace(lower(genre), ' ', '-')
                                    HAVING
                                        SUM(CASE WHEN g_nxg = 'M' OR g_nxg = 'F' THEN 1 ELSE 0 END) >= 1000
                                '''


# given that there may be more female than male users, we'll need a baseline 
# to compare our results to.
USER_GENDER_SPLIT_BASELINE_QUERY = '''
                                    SELECT
                                        -- male count
                                        SUM(CASE WHEN g_nxg = 'M' THEN 1 ELSE 0 END),
                                        -- female count
                                        SUM(CASE WHEN g_nxg = 'F' THEN 1 ELSE 0 END)
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
                                            -- male count
                                            SUM(CASE WHEN g_comp = 'M' THEN 1 ELSE 0 END) * 1.0,
                                            -- female count
                                            SUM(CASE WHEN g_comp = 'F' THEN 1 ELSE 0 END) * 1.0
                                        FROM
                                            pound
                                        INNER JOIN
                                            g_pound ON pound.author_id = g_pound.author_id
                                        WHERE 
                                            -- more confident gender prediction
                                            descr IS NOT NULL
                                        AND
                                            -- individual author has at least 100 ratings from Goodreads users
                                            rating_count >= 100
                                        AND
                                            top_genres[1] IS NOT NULL
                                     '''


def main():
    data = {
        'author': pd.DataFrame(),
        'user': pd.DataFrame() 
    }

    # GET DATA
    PG_STRING = os.getenv('PG_STRING')
    with psycopg.connect(conninfo=PG_STRING,
                         autocommit=True) as conn:
        with conn.cursor() as cur:
            for grp in ['author', 'user']:
                q_split = AUTHOR_GENRE_GENDER_SPLIT_QUERY if grp == 'author' else USER_GENRE_GENDER_SPLIT_QUERY
                q_baseline = AUTHOR_GENDER_SPLIT_BASELINE_QUERY if grp == 'author' else USER_GENDER_SPLIT_BASELINE_QUERY
                # get genre gender splits
                cur.execute(q_split)
                genre_splits = [
                    {
                        'genre': r[0],
                        'num_men': r[1],
                        'num_women': r[2],
                        'sample_size': r[1] + r[2],
                        'share_men': r[1] / (r[1] + r[2]) * 100,
                        'share_women': r[2] / (r[1] + r[2]) * 100
                    } for r in cur.fetchall()
                ]
                # get baseline gender splits
                cur.execute(q_baseline)
                r = cur.fetchone()
                baseline_dat = {
                    'genre': 'OVERALL POPULATION',
                    'num_men': r[0],
                    'num_women': r[1],
                    'sample_size': r[0] + r[1],
                    'share_men': r[0] / (r[0] + r[1]) * 100,
                    'share_women': r[1] / (r[0] + r[1]) * 100
                }
                genre_splits.append(baseline_dat)
                # get into df
                df = pd.DataFrame(data=genre_splits)
                df = df.sort_values(by='share_men')
                data[grp] = df
    
    # GENERATE FIGURES
    # first, gender splits
    for grp, df in data.items():
        df['hover_text_men'] = (
            '<b>' +  df['genre'] + '</b><br>' + 
            '<b>Male Share</b>: ' + df['share_men'].astype(float).round(2).astype(str) + '%<br>' + 
            '<b>Sample Size</b>: ' + df['sample_size'].astype(str)
        )
        df['hover_text_women'] = (
            '<b>' +  df['genre'] + '</b><br>' + 
            '<b>Female Share</b>: ' + df['share_women'].astype(float).round(2).astype(str) + '%<br>' + 
            '<b>Sample Size</b>: ' + df['sample_size'].astype(str)
        )
        
        fig = go.Figure(
            data=[
                go.Bar(
                    orientation='h',
                    name='Men',
                    x=df['share_men'],
                    y='<b>' + df['genre'] + '</b> ',
                    hovertext=df['hover_text_men'],
                    hovertemplate='%{hovertext}<extra></extra>',
                    marker={
                        'color': "#B2DCEA",
                        'opacity': 0.75
                    }
                ),
                go.Bar(
                    orientation='h',
                    name='Women',
                    x=df['share_women'],
                    y='<b>' + df['genre'] + '</b> ',
                    hovertext=df['hover_text_women'],
                    hovertemplate='%{hovertext}<extra></extra>',
                    marker={
                        'color': "#E9C9E1",
                        'opacity': 0.75
                    }
                )

            ]
        )
        fig.update_layout(barmode='stack', title_text=f'<b>Goodreads {grp.title()} Genre Gender Split</b>')
        fig.add_vline(
            x=df.loc[df['genre'] == 'OVERALL POPULATION', 'share_men'].to_list()[0],
            line_dash='dash',
            line_width=2,
            annotation_text='<b>OVERALL POPULATION</b>',
            annotation_position='top right'
        )
        fig.update_xaxes(title_text = f'Goodreads {grp.title()} Gender Split', 
                        range=[0,100],
                        ticksuffix='%')
        fig.update_yaxes(title_text = f'Goodreads {grp.title()} Genre')
        
        fig.write_html(file=os.path.join('visualizations', 'figures', f'gsplit_{grp}.html'))
    
    # second, within-gender composition
    for grp, df in data.items():
        df_within = df.loc[df['genre'] != 'OVERALL POPULATION'].copy()
        df_within['genre_broad'] = df_within['genre'].apply(broad_genre)
        male_comp = df_within.groupby('genre_broad')['num_men'].sum().reset_index().sort_values(by='genre_broad')
        female_comp = df_within.groupby('genre_broad')['num_women'].sum().reset_index().sort_values(by='genre_broad')

        fig = make_subplots(rows=1, cols=2, 
                            column_titles=['<b>Men</b>', '<b>Women</b>'],
                            specs=[[{'type':'domain'}, {'type':'domain'}]])
        fig.add_trace(
            go.Pie(
                    labels='<b>' + male_comp['genre_broad'] + '</b>',
                    values=male_comp['num_men'],
                    marker_colors=male_comp['genre_broad'].map(GENRE_COLORS),
                    name='Men'
                ),
            row=1, col=1
        )
        fig.add_trace(
            go.Pie(
                    labels='<b>' + female_comp['genre_broad'] + '</b>',
                    values=female_comp['num_women'],
                    marker_colors=female_comp['genre_broad'].map(GENRE_COLORS),
                    name='Women'
                ),
            row=1, col=2
        )
        fig.update_traces(hoverinfo='label+percent')
        fig.update_layout(title_text=f'<b>Goodreads {grp.title()} Genre Gender Composition</b>')
        
        fig.write_html(file=os.path.join('visualizations', 'figures', f'gcomp_{grp}.html'))
        

if __name__ == '__main__':
    main()