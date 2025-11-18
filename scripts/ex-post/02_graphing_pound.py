import os

import psycopg
import networkx as nx
from ipysigma import Sigma
from dotenv import load_dotenv
load_dotenv()


GET_EDGES_QUERY = '''
                    WITH unnested_sims(a_id, s_id) AS (
                        SELECT
                            pound.author_id,
                            unnest(pound.sim_authors[:10])
                        FROM 
                            pound 
                        WHERE 
                            rating_count >= %s
                    )

                    SELECT 
                        u_s.a_id,
                        u_s.s_id
                    FROM
                        unnested_sims AS u_s
                    INNER JOIN
                        pound ON u_s.s_id = pound.author_id
                  '''

GET_NODES_QUERY = '''
                    WITH unnested_sims(a_id, s_id) AS (
                        SELECT
                            pound.author_id,
                            unnest(pound.sim_authors[:10])
                        FROM 
                            pound 
                        WHERE 
                            rating_count >= %s
                    ),
                    edges(a_id, s_id) AS (
                        SELECT 
                            u_s.a_id,
                            u_s.s_id
                        FROM
                            unnested_sims AS u_s
                        INNER JOIN
                            pound ON u_s.s_id = pound.author_id
                    )

                    SELECT
                        author_id,
                        author_name
                    FROM
                        pound
                    WHERE
                        author_id IN (
                            SELECT a_id FROM edges
                            UNION
                            SELECT s_id FROM edges
                        )
                  '''

def main():
    # connection string
    PG_STRING = os.getenv("PG_STRING")
    # rating count cutoff
    AT_LEAST_N_RATINGS = (20000,)

    graph = nx.Graph()

    with psycopg.connect(PG_STRING) as conn:
        with conn.cursor() as cur:
            
            # get edges 
            cur.execute(GET_EDGES_QUERY, AT_LEAST_N_RATINGS)
            while True:
                r_edges = cur.fetchone()
                if not r_edges:
                    break
                graph.add_edge(r_edges[0], r_edges[1])
            
            # get nodes
            cur.execute(GET_NODES_QUERY, AT_LEAST_N_RATINGS)
            while True:
                r_nodes = cur.fetchone()
                if not r_nodes:
                    break
                graph.add_node(r_nodes[0], label=r_nodes[1])
    
    
    Sigma.write_html(graph=graph, 
                     path='outit.html', 
                     fullscreen=True,
                     background_color='black')


if __name__ == '__main__':
    main()
