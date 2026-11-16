"""Read-only, bounded book/author recommendation graphs."""
import argparse
from pathlib import Path

import networkx as nx
from ipysigma import Sigma
from psycopg import sql

from guide2kulchur.pipeline.common import add_database, database, output_path, positive
from guide2kulchur.pipeline.sources import TABLES, normalize_id


def graph_from_rows(rows):
    graph = nx.Graph()
    for identifier, name, rating_count, similar in rows:
        graph.add_node(identifier, label=name, rating_count=rating_count or 0)
    for identifier, _, _, similar in rows:
        for neighbor in similar or []:
            if neighbor in graph and neighbor != identifier:
                graph.add_edge(identifier, neighbor)
    return graph


def select_rows(conn, args):
    table, identifier = TABLES[args.kind]
    name, links = ('title', 'sim_books') if args.kind == 'book' else ('author_name', 'sim_authors')
    columns = sql.SQL(', ').join(map(sql.Identifier, [identifier, name, 'rating_count', links]))
    prefix = sql.SQL('SELECT {} FROM public.{}').format(columns, sql.Identifier(table))
    params = [args.min_ratings, args.genre, args.genre]
    query = prefix + sql.SQL(''' WHERE rating_count >= %s AND (%s::text IS NULL OR
        EXISTS (SELECT 1 FROM unnest(top_genres) g WHERE g ILIKE %s))''')
    if args.item_id:
        item = normalize_id(args.item_id, args.kind)
        seed = conn.execute(prefix + sql.SQL(' WHERE {} = %s').format(sql.Identifier(identifier)), (item,)).fetchone()
        if not seed:
            return []
        query += sql.SQL(' AND {} = ANY(%s)').format(sql.Identifier(identifier))
        params.append(seed[3] or [])
        params.append(max(0, args.limit - 1))
        rows = conn.execute(query + sql.SQL(' ORDER BY rating_count DESC, {} LIMIT %s').format(sql.Identifier(identifier)), params).fetchall()
        return [seed] + [row for row in rows if row[0] != item]
    params.append(args.limit)
    return conn.execute(query + sql.SQL(' ORDER BY rating_count DESC, {} LIMIT %s').format(sql.Identifier(identifier)), params).fetchall()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=('book', 'author'), required=True)
    parser.add_argument('--genre', help='case-insensitive SQL LIKE pattern, e.g. %%fiction%%')
    parser.add_argument('--min-ratings', type=positive, default=100)
    parser.add_argument('--limit', type=positive, default=1000, help='maximum nodes; only links among selected nodes are included')
    parser.add_argument('--item-id', help='one-hop graph around an ID/URL instead of the top-rated sample')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--overwrite', action='store_true')
    add_database(parser)
    args = parser.parse_args(argv)
    if args.output.exists() and not args.overwrite:
        parser.error('output exists; choose another path or --overwrite')
    with database(args.env_file) as conn:
        graph = graph_from_rows(select_rows(conn, args))
    path = output_path(args.output, overwrite=args.overwrite)
    Sigma.write_html(graph, str(path), fullscreen=True, node_size=graph.degree,
                     hide_edges_on_move=True, default_edge_type='curve')
    print(f'{graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges -> {path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
