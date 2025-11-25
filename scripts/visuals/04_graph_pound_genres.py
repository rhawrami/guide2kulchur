'''
This script generates network graphs for authors using...
- networkx to build the graph object: https://networkx.org/
- ipysigma (python jupyter widget for sigma.js): https://github.com/medialab/ipysigma

Graphs will be made for the following genres:
- Young Adult
- Fiction
- Nonfiction
- Philosophy
- Politics
- Religion
- Sci-Fi
- Manga
- Poetry
'''

import os

import psycopg
import networkx as nx
from ipysigma import Sigma
from dotenv import load_dotenv
load_dotenv()


STYLING = '''
<style>
    body {
        background-color: #f8f9fa !important;
    }
    
    .ipysigma-widget {
        background-color: #f8f9fa !important;
        border: 1px solid #dee2e6 !important;
        color: #333333 !important;
    }
    
    .ipysigma-widget .ipysigma-button,
    .ipysigma-widget .ipysigma-graph-description,
    .ipysigma-widget .ipysigma-information-display {
        background-color: #ffffff !important;
        border: 1px solid #dee2e6 !important;
        color: #333333 !important;
    }
    
    .ipysigma-widget .ipysigma-button:hover {
        border-color: #adb5bd !important;
        background-color: #f1f3f5 !important;
    }
    
    .ipysigma-widget .choices__inner {
        background-color: #ffffff !important;
        border: 1px solid #dee2e6 !important;
        color: #333333 !important;
    }
    
    .ipysigma-widget .choices__list {
        background-color: #ffffff !important;
    }
    
    .ipysigma-widget .choices__item {
        color: #333333 !important;
    }
    
    .ipysigma-widget hr {
        background-color: #dee2e6 !important;
    }
    
    .ipysigma-widget svg {
        fill: #495057 !important;
    }
    
    .ipysigma-widget .ipysigma-string {
        color: #d63384 !important;
    }
    
    .ipysigma-widget .ipysigma-number {
        color: #0d6efd !important;
    }
    
    .ipysigma-widget .ipysigma-boolean,
    .ipysigma-widget .ipysigma-keyword {
        color: #6610f2 !important;
    }
    
    /* Category items in legend */
    .ipysigma-widget .category.evicted .category-value {
        color: #adb5bd !important;
    }
    
    .ipysigma-download-controls {
        display: none !important;
        visibility: hidden !important;
    }

    .ipysigma-widget .choices__item--selectable.is-highlighted,
    .ipysigma-widget .choices__list--dropdown .choices__item--selectable.is-highlighted {
        background-color: #e9ecef !important;  
        color: #333333 !important;
    }
</style>
'''

GENRE_CFG = {
    'young-adult': {
        'out_suffix': 'YoungAdult',
        'query_params': (40000, '[Yy]oung Adult')
    },
    'fiction': {
        'out_suffix': 'Fiction',
        'query_params': (200000, 'Fiction')
    },
    'nonfiction': {
        'out_suffix': 'Nonfiction',
        'query_params': (50000, 'Nonfiction')
    },
    'novel': {
        'out_suffix': 'Novel',
        'query_params': (200000, 'Novel')
    },
    'philosophy': {
        'out_suffix': 'Philosophy',
        'query_params': (5000, 'Philosophy')
    },
    'politics': {
        'out_suffix': 'Politics',
        'query_params': (10000, 'Politic')
    },
    'religion': {
        'out_suffix': 'Religion',
        'query_params': (10000, 'Religio(n|us)')
    },
    'sci-fi-fantasy': {
        'out_suffix': 'SciFiFantasy',
        'query_params': (50000, 'Sci-?[Ff]i|Science Fiction|Fantasy')
    },
    'manga': {
        'out_suffix': 'Manga',
        'query_params': (2000, 'Manga')
    },
    'poetry': {
        'out_suffix': 'Poetry',
        'query_params': (10000, 'Poet')
    }
}


GET_EDGES_QUERY = '''
                    SELECT
                        author_id,
                        unnest(sim_authors)
                    FROM
                        pound
                    WHERE
                        rating_count >= %s
                    AND
                        top_genres[1] ~ %s
                  '''


GET_NODES_QUERY = '''
                    WITH edges_q(a_id, s_id) AS (
                        SELECT
                            author_id,
                            unnest(sim_authors)
                        FROM
                            pound
                        WHERE
                            rating_count >= %s
                        AND
                            top_genres[1] ~ %s
                    ),
                    all_nodes(a_id) AS (
                        (SELECT DISTINCT a_id FROM edges_q)
                        UNION
                        (SELECT  DISTINCT s_id FROM edges_q)
                    )
                    
                    SELECT
                        author_id,
                        author_name
                    FROM
                        pound
                    INNER JOIN
                        all_nodes ON pound.author_id = all_nodes.a_id
                  '''


def main():
    # connection string
    PG_STRING = os.getenv("PG_STRING")

    with psycopg.connect(PG_STRING) as conn:
        with conn.cursor() as cur:
            for name, g in GENRE_CFG.items():
                gr = nx.Graph()
                # get edges 
                cur.execute(GET_EDGES_QUERY, g['query_params'])
                while True:
                    r_edges = cur.fetchone()
                    if not r_edges:
                        break
                    gr.add_edge(r_edges[0], r_edges[1])
                
                # get nodes
                cur.execute(GET_NODES_QUERY, g['query_params'])
                while True:
                    r_nodes = cur.fetchone()
                    if not r_nodes:
                        break
                    gr.add_node(r_nodes[0], label=r_nodes[1])

                out_file = os.path.join('visualizations', 'graphs', f'pnd_genre_{g['out_suffix']}.html')
                Sigma.write_html(
                    graph=gr, 
                    path=out_file, 
                    fullscreen=True,
                    start_layout=30,
                    node_metrics={"similarity": "louvain"}, 
                    node_color="similarity",
                    node_color_palette='Pastel1',
                    node_size_range=(3, 21),
                    max_categorical_colors=30,
                    default_edge_type='curve',
                    label_font="cursive",
                    default_edge_color="#E8E6E6FF",
                    node_border_color_from='node',
                    node_label_size=gr.degree,
                    node_label_size_range=(10,30),
                    default_node_label_color="#000000",
                    node_size=gr.degree,
                    hide_edges_on_move=True,
                    hide_info_panel=True
                )
                
                # add styling
                rw_html = ''
                with open(out_file, 'r') as o_f:
                    for line in o_f:
                        if '<title>IPyWidget export</title>' in line:
                            line = f'<title>Authors by Genre ({g['out_suffix']})</title>\n{STYLING}\n'
                        rw_html += line
                with open(out_file, 'w') as o_f:
                    o_f.write(rw_html)

                print(f'{name} completed - {gr.number_of_nodes()} nodes, {gr.number_of_edges()} edges.')


if __name__ == '__main__':
    main()
