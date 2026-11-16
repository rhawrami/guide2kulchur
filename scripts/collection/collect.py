"""Collect books, authors, or users. Preview by default; see --help."""
import sys
from guide2kulchur.pipeline.cli import main

if __name__ == '__main__':
    raise SystemExit(main(['collect', *sys.argv[1:]]))
