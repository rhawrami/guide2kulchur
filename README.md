# guide2kulchur

<img src="./img/g2k_logo.svg" alt="logo test" width="200"/>

## description
`guide2kulchur` is a project focused on studying the reading and writing patterns across the world, along a number of dimensions like time, geography and genre. `guide2kulchur` specifically is focused on the data collection for this project, primarily from Goodreads. Goodreads has a rich set of book, author and user data, and the social interactions between each of these sources, making it perfect for this sort of project. Obviously, none of the data pulled here is used for commercial purposes.

## [guide2kulchur.xyz](https://guide2kulchur.xyz)
You can see the fruits of this project at [guide2kulchur.xyz](https://guide2kulchur.xyz), including interactive visualizations on the data collected. See below for a preview of the visualizations:

![](./img/g2k_graph.png)

![](./img/g2k_gsplit.png)

![](./img/g2k_map.png)

You can look at the [Methods](https://guide2kulchur.xyz/info/methods.html) and [About](https://guide2kulchur.xyz/info/about.html) pages for more information.

## development
Dependencies are managed with [uv](https://docs.astral.sh/uv/). From the repository root:

```bash
uv sync --locked
```

uv downloads the Python version pinned in `.python-version` if needed and creates a local `.venv/`;
no manual activation is required.
`pyproject.toml` defines the dependencies, and `uv.lock` records the resolved versions.

Run scripts from the repository root, for example:

```bash
uv run python -m unittest discover -s tests -v
```

Database scripts require `PG_STRING` in the environment or a local `.env` file, plus an existing
PostgreSQL database with the appropriate schema and data. Collection scripts make network requests
and write to that database; review their configuration before running them.

Use `uv add <package>` or `uv remove <package>` to change dependencies, and commit both
`pyproject.toml` and `uv.lock`. To update locked dependencies, run `uv lock --upgrade`, then `uv sync`.

## repository
There are a number of directories in this repository; below are the most important:

- [guide2kulchur](./guide2kulchur/): the actual library created for pulling goodreads data, parsing it, and loading it into a Postgres database; includes two subdirectories:
    - [privateer](./guide2kulchur/privateer/): pulling publicly available book/author/user data from Goodreads, and parsing a number of fields
    - [engineer](./guide2kulchur/engineer/): pulling data in a structured manner, and loading it into a database
- [scripts/archive](./scripts/archive/): original collection, enrichment, and visualization scripts, preserved with their original directory names and numbered ordering. These are historical reference, not maintained entry points; their original contents and ordering are preserved.
- [db](./db): database schema and migration files; you can see the [inital schema](./db/init_schema.sql), and the [number of changes](./db/migrations/) made throughout the way. You can see the schema visualization below (made with DBeaver). You will see that there aren't any references/connections between tables; given the nature of the data extraction, reference conditions couldn't be enforced (e.g., can't confirm that author of book "The Master and Margarita", Mikhail Bulgakov, will be present in the author data table). Nonetheless, implicit references via certain attributes can still be seen clearly, by attributes like `book_id` and `author_id` for example:

![](./img/g2k_dbschema.png)

## [kulchur](https://github.com/rhawrami/kulchur/)
If you'd like to work with publicly available Goodreads data, **in a respectful and non-commerical intention**, you can use `kulchur`, the python package made through the development of this project. You can view the [repository here](https://github.com/rhawrami/kulchur/), and install with `pip`:
```bash
$ pip install kulchur
```

## contact
You can contact me at ravanhawrami[at]gmail[dot]com for any inquiries. Thanks!
