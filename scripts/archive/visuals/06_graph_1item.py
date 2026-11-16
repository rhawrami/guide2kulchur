'''
This script generates network graphs for authors using...
- networkx to build the graph object: https://networkx.org/
- ipysigma (python jupyter widget for sigma.js): https://github.com/medialab/ipysigma

Only authors/books with at least 1000 ratings are shown. In order to get an author's/book's
network, we do the following (using "item_id" as the Goodreads ID of a book/author):
1. find all items (unnest first) with "item_id" in their similar_items array column, as well as "item_id"'s array column contents
2. filter the items above to those with at least 1000 ratings
3. find all connections between the items included

This way, for example, we can see that both Thucydides and Homer have connections to Plato, and that Thucydides and Homer have a 
connection to each other. Essentially, we are taking a pool of authors/books that have a similarity to a specific item, then seeing
if any of those similar items have similarities to each other.
'''

import os
import sys

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


def main():
    # pass the book/author ID as the only argument, with an 'a' or 'b' as the prefix
    # for example (find all authors similar to Plato):
        # $ python3 scripts/visuals/06_graph_1item.py a879
    if len(sys.argv) > 2 or len(sys.argv) < 2:
        print('error: only provide one argument of the form [ba]\\d+')
        exit(1)
    if not sys.argv[1].startswith('a') and not sys.argv[1].startswith('b'):
        print('error: author (a) or book (b) ID must start with the letter identifier:e.g., [ba]\\d+')
        exit(1)

    TABLE_CFG = {
        'a': {
            'table_name': 'pound',
            'prefix': 'author',
            'table_name_short': 'pnd'
        },
        'b': {
            'table_name': 'alexandria',
            'prefix': 'book',
            'table_name_short': 'alx'
        }
    }
    if sys.argv[1].startswith('a'):
        item_type = 'a'
    else:
        item_type = 'b'
    t_name = TABLE_CFG[item_type]['table_name']
    prefix = TABLE_CFG[item_type]['prefix']
    t_name_short = TABLE_CFG[item_type]['table_name_short']
    item_id = sys.argv[1][1:]
    name_var = 'title' if item_type == 'b' else 'author_name'

    get_edges_query = f'''
                    WITH all_conns(i_id, s_id) AS (
                        SELECT
                            {prefix}_id,
                            unnest(sim_{prefix}s)
                        FROM
                            {t_name}
                        WHERE
                            rating_count >= 1000
                        AND (
                                '{item_id}' = ANY(sim_{prefix}s)
                            OR
                                {prefix}_id = '{item_id}'
                            )
                        ),
                    relevant_conns(i_id, s_id) AS (
                        SELECT
                            i_id,
                            s_id
                        FROM 
                            all_conns
                        WHERE 
                            i_id = '{item_id}'
                        OR
                            s_id = '{item_id}'
                    ),
                    relevant_nodes(i_id) AS (
                        (SELECT DISTINCT i_id FROM relevant_conns)
                        UNION
                        (SELECT DISTINCT s_id FROM relevant_conns)
                    )
                    SELECT 
                        i_id,
                        s_id
                    FROM 
                        all_conns
                    WHERE
                        i_id IN (SELECT i_id FROM relevant_nodes)
                    AND
                        s_id IN (SELECT i_id FROM relevant_nodes)
                  '''
    
    get_nodes_query = f'''
                    WITH all_conns(i_id, s_id) AS (
                        SELECT
                            {prefix}_id,
                            unnest(sim_{prefix}s)
                        FROM
                            {t_name}
                        WHERE
                            rating_count >= 1000
                        AND (
                                '{item_id}' = ANY(sim_{prefix}s)
                            OR
                                {prefix}_id = '{item_id}'
                            )
                        ),
                    relevant_conns(i_id, s_id) AS (
                        SELECT
                            i_id,
                            s_id
                        FROM 
                            all_conns
                        WHERE 
                            i_id = '{item_id}'
                        OR
                            s_id = '{item_id}'
                    ),
                    relevant_nodes(i_id) AS (
                        (SELECT DISTINCT i_id FROM relevant_conns)
                        UNION
                        (SELECT DISTINCT s_id FROM relevant_conns)
                    )
                    
                    SELECT
                        {prefix}_id,
                        {name_var}
                    FROM
                        {t_name}
                    INNER JOIN
                        relevant_nodes ON {t_name}.{prefix}_id = relevant_nodes.i_id
                  '''
    
    gr = nx.Graph()

    # connection string
    PG_STRING = os.getenv("PG_STRING")
    with psycopg.connect(PG_STRING) as conn:
        with conn.cursor() as cur:

            # get book/author name
            get_name_query = f"SELECT {name_var} FROM {t_name} WHERE {prefix}_id = '{item_id}'"
            cur.execute(get_name_query)
            item_name = cur.fetchone()
            if not item_name:
                print(f'error: {prefix} {item_id} not in database.')
                exit(1)
            item_name = item_name[0]

            # get edges 
            cur.execute(get_edges_query)
            while True:
                r_edges = cur.fetchone()
                if not r_edges:
                    break
                gr.add_edge(r_edges[0], r_edges[1])
            
            # get nodes
            cur.execute(get_nodes_query)
            while True:
                r_nodes = cur.fetchone()
                if not r_nodes:
                    break
                gr.add_node(r_nodes[0], label=r_nodes[1])

            out_file = os.path.join('visualizations', 'graphs', f'{t_name_short}_1item_{item_name.replace(' ', '-').lower()}.html')
            Sigma.write_html(
                graph=gr, 
                path=out_file, 
                fullscreen=True,
                start_layout=30,
                node_metrics={"similarity": "louvain"}, 
                node_color="similarity",
                node_color_palette='Pastel1',
                node_size_range=(5, 25),
                max_categorical_colors=30,
                default_edge_type='curve',
                label_font="cursive",
                default_edge_color="#E8E6E6FF",
                node_border_color_from='node',
                node_label_size=gr.degree,
                node_label_size_range=(15,35),
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
                        line = f'<title>Network of {item_name}</title>\n{STYLING}\n'
                    rw_html += line
            with open(out_file, 'w') as o_f:
                o_f.write(rw_html)
            print(f'{prefix} {item_id} ({item_name}) NETWORK WRITTEN TO:\n{out_file}')


if __name__ == '__main__':
    main()
