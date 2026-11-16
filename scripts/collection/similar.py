"""Fill book/author recommendation arrays. Preview by default; see --help."""
import sys
from guide2kulchur.pipeline.cli import main

if __name__ == '__main__':
    raise SystemExit(main(['similar', *sys.argv[1:]]))
