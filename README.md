# Condenser

Condenser is a config-driven database subsetting tool for Postgres and MySQL.

Subsetting data is the process of taking a representative sample of your data in a manner that preserves the integrity of your database, e.g., give me 5% of my users. If you do this naively, e.g., just grab 5% of all the tables in your database, most likely, your database will break foreign key constraints. At best, you’ll end up with a statistically non-representative data sample.

One common use-case is to scale down a production database to a more reasonable size so that it can be used in staging, test, and development environments. This can be done to save costs and, when used in tandem with PII removal, can be quite powerful as a productivity enhancer. Another example is copying specific rows from one database and placing them into another while maintaining referential integrity.

You can find more details about how we built this [here](https://www.tonic.ai/blog/condenser-a-database-subsetting-tool).

# Need to Subset a Large Database?

Our open-source tool can subset databases up to 10GB, but it will struggle with larger databases. Our premium database subsetter can, among other things, subset multi-TB databases with ease. If you're interested find us at [hello@tonic.ai](mailto:hello@tonic.ai).

# TLDR Setup

Four steps to setup, assuming Python 3.5+:

1. Download this repo. You can clone the repo or Download it as a zip. Scroll up, it's the green button that says "Clone or download".
2. Download the required Python modules. You can use [`pip`](https://pypi.org/project/pip/) for easy installation. The required modules are `toposort`, `psycopg2-binary`, and `mysql-connector-python`.
```
$ pip install toposort
$ pip install psycopg2-binary
$ pip install mysql-connector-python
```

3. Setup your configuration. See the details below. The simplest thing is to copy `example-config.json` to `config.json` and fill out your source and destination database connection details, as well as subsetting goals. There may be more required configuration depending on your database, but simple databases should be easy.
4. Run! `$ python direct_subset.py`

# Config

Configuration must exist in `config.json`. There is an example configuration provided in `example-config.json`. Most of the configuration is straightforward: source and destination DB connection details and subsetting settings. There are three fields that desire some additional attention.

The first is `initial_targets`. This is where you tell the subsetter to begin the subset. You can specify any number of tables as an initial target, and provide either a percent goal (e.g. 5% of the `users` table) or a WHERE clause.

Next is `dependency_breaks`. The best way to get a full understanding of this is to read our [blog post](https://www.tonic.ai/blog/condenser-a-database-subsetting-tool). But if you want a TLDR, it's this: The subsetting tool cannot operate on databases with cycles in their foreign key relationships. (Example: Table `events` references `users`, which references `company`, which references `events`, a cycle exists if you think about the foreign keys as a directed graph.) If your database has a foreign key cycle (any many do), have no fear! This field lets you tell the subsetter to ignore certain foreign keys, and essentially remove the cycle. You'll have to know a bit about your database to use this field effectively. The tool will warn you if you have a cycle that you haven't broken.

The last is `fk_augmentation`. Databases frequently have foreign keys that are not codified as constraints on the database, these are implicit foreign keys. For a subsetter to create useful subsets if needs to know about this implicit constraints. This field lets you essentially add foreign keys to the subsetter that the DB doesn't have listed as a constraint.

Below we describe the use of all configuration parameters, but the best place to start for the exact format is `example-config.json`.

`db_type`: The type of the databse to subset. Valid values are `"postgres"` or `"mysql"`.

`source_db_connection_info`: Source database connection details. These are recorded as a JSON object with the fields `user_name`, `host`, `db_name`, `ssl_mode`, `password` (optional), and `post`. If `password` is omitted, then you will be prompted for a password. See `example-config.json` for details.

`destination_db_connection_info`: Destination database connection details. Same fields as `source_db_connection_info`.

`initial_targets`: JSON array of JSON objects. The inner object must contain a `target` field, which is a target table, and either a `where` field or a `percent` field. The `where` field is used to specify a WHERE clause for the subsetting. The `percent` field indicates we want a specific percentage of the target table; it is equivalent to `"where": "random() < <percent>/100.0"`.

`passthrough_tables`: Tables that will be copied to destination database in whole. The value is a JSON array of strings, of the form `"<schema>.<table>"` for Postgres and `"<database>.<table>"` for MySQL.

`excluded_tables`: Tables that will be excluded from the subset. The table will exist in the output, but contain no rows. The value is a JSON array of strings, of the form `"<schema>.<table>"` for Postgres and `"<database>.<table>"` for MySQL.

`fk_augmentation`: Additional foreign keys that, while not represented as constraints in the database, are logically present in the data. Foreign keys listed in `fk_augmentation` are unioned with the foreign keys provided by constraints in the database. `fk_augmentation` is useful when there are foreign keys existing in the data, but not represented in the database. The value is a JSON array of JSON objects. See `example-config.json` for details.

`dependency_breaks`: An array containg a JSON object with *"fk_table"* and *"target_table"* fields of table relationships to be ignored in order to break cycles

`keep_disconnected_tables`: If `true` tables that the subset target(s) don't reach, when following foreign keys, will be copied 100% over. If it's `false` then their schema will be copied but the table contents will be empty. Put more mathematically, the tables and foreign keys create a graph (where tables are nodes and foreign keys are directed edges) disconnected tables are the tables in components that don't contain any targets. This setting decides how to import those tables.

# Running

Almost all the configuration is in the `config.json` file, so running is as simple as

```
$ python direct_subset.py
```

Two commandline arguements are supported:

`-v`: Verbose output. Useful for performance debugging. Lists almost every query made, and it's speed.

`--no-constraints`: For Postgres this will not add constraints found in the source database to the destination database. This option has no effect for MySQL.

# Requirements

Reference the requirements.txt file for a list of required python packages.  Also, please note that python3.6 is required.
