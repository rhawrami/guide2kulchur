"""Save new genre seed lists or sitemap IDs; preview unless --execute is supplied."""
import argparse
import asyncio
import gzip
import json
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

from guide2kulchur.engineer.plotinus import Plotinus
from guide2kulchur.pipeline.common import nonnegative, output_path, positive
from guide2kulchur.pipeline.http import CheckedSession, Client, ResponseError
from guide2kulchur.pipeline.sources import normalize_id


def locations(text):
    return [node.text.strip() for node in ET.fromstring(text).iter() if node.tag.rsplit('}', 1)[-1] == 'loc' and node.text]


def sitemap_url(value):
    parsed = urlparse(value)
    if parsed.scheme != 'https' or parsed.hostname not in ('www.goodreads.com', 'goodreads.com'):
        raise ValueError('Sitemap URL must be an HTTPS Goodreads URL')
    return value


async def discover(args):
    async with Client(delay=args.request_delay, timeout=args.timeout) as client:
        proxy = CheckedSession(client)
        if args.mode == 'genres':
            results = {}
            puller = Plotinus()
            method = puller.get_top_shelf if args.shelf == 'top-shelved' else puller.get_most_read_this_week
            for genre in args.genre[:args.limit]:
                proxy.error = None
                values = await method(session=proxy, genre_name=genre, num_attempts=1, see_progress=False)
                if proxy.error:
                    raise proxy.error
                if values is None:
                    raise ValueError(f'No seed data parsed for {genre}; output not replaced')
                results[genre] = list(dict.fromkeys(values))
            return {'results': results}
        index = f'https://www.goodreads.com/siteindex.{args.kind}.xml'
        async with proxy.get(index) as response:
            pages = locations(await response.text())
        ids = set()
        for page in pages[:args.max_pages]:
            async with proxy.get(sitemap_url(page)) as response:
                content = await response.read()
            if content.startswith(b'\x1f\x8b'):
                content = gzip.decompress(content)
            for url in locations(content):
                try:
                    ids.add(normalize_id(url.replace('http://', 'https://', 1), args.kind))
                    if len(ids) >= args.limit:
                        break
                except ValueError:
                    continue
            if len(ids) >= args.limit:
                break
        return sorted(ids, key=int)[:args.limit]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='mode', required=True)
    for mode in ('genres', 'sitemap'):
        sub = subs.add_parser(mode)
        sub.add_argument('--output', type=Path, required=True)
        sub.add_argument('--limit', type=positive, default=10 if mode == 'genres' else 1000)
        sub.add_argument('--request-delay', type=nonnegative, default=2)
        sub.add_argument('--timeout', type=positive, default=30)
        sub.add_argument('--execute', action='store_true', help='enable network requests and write the output file')
        sub.add_argument('--overwrite', action='store_true')
        if mode == 'genres':
            sub.add_argument('--genre', nargs='+', required=True)
            sub.add_argument('--shelf', choices=('top-shelved', 'most-read'), default='most-read')
        else:
            sub.add_argument('--kind', choices=('author', 'user'), required=True)
            sub.add_argument('--max-pages', type=positive, default=1)
    args = parser.parse_args(argv)
    if args.mode == 'genres' and args.output.suffix != '.json':
        parser.error('genre output must have a .json suffix')
    if args.mode == 'sitemap' and args.output.suffix not in ('.txt', '.gz'):
        parser.error('sitemap output must have a .txt or .gz suffix')
    if not args.execute:
        print(json.dumps({'mode': 'preview', 'operation': args.mode, 'limit': args.limit, 'output': str(args.output)}))
        return 0
    if args.output.exists() and not args.overwrite:
        parser.error('output exists; choose another path or --overwrite')
    try:
        data = asyncio.run(discover(args))
    except ResponseError as exc:
        print(str(exc))
        return 2 if exc.blocked else 1
    path = output_path(args.output, overwrite=args.overwrite)
    if args.mode == 'genres':
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    elif path.suffix == '.gz':
        with gzip.open(path, 'wt') as output:
            output.write('\n'.join(data) + '\n')
    else:
        path.write_text('\n'.join(data) + '\n')
    print(f'Saved {path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
