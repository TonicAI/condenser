# Condenser

Condenser is a config-driven database subsetting tool for Postgres and MySQL.

Subsetting data is the process of taking a representative sample of your data in a manner that preserves the integrity of your database, e.g., give me 5% of my users. If you do this naively, e.g., just grab 5% of all the tables in your database, most likely, your database will break foreign key constraints. At best, you’ll end up with a statistically non-representative data sample.

One common use-case is to scale down a production database to a more reasonable size so that it can be used in staging, test, and development environments. This can be done to save costs and, when used in tandem with PII removal, can be quite powerful as a productivity enhancer. Another example is copying specific rows from one database and placing them into another while maintaining referential integrity.

You can find more details about how we built this [here](https://www.tonic.ai/blog/condenser-a-database-subsetting-tool) and [here](https://www.tonic.ai/blog/condenser-v2/).

## Need to Subset a Large Database?

Our open-source tool can subset databases up to 10GB, but it will struggle with larger databases. Our premium database subsetter can, among other things (graphical UI, job scheduling, fancy algorithms), subset multi-TB databases with ease. If you're interested find us at [hello@tonic.ai](mailto:hello@tonic.ai).

# Installation

Five steps to install, assuming Python 3.6+:

1. [Install Poetry](https://python-poetry.org/docs/#installation)

2. Install Postgres and/or MySQL database tools. For Postgres we need `pg_dump` and `psql` tools; they need to be on your `$PATH` or point to them with `$POSTGRES_PATH`. For MySQL we need `mysqldump` and `mysql`, they can be on your `$PATH` or point to them with `$MYSQL_PATH`.

3. Clone project locally:
```
$ git clone https://github.com/TonicAI/condenser.git
$ cd condenser
```

4. Install project:
```
$ poetry shell
$ poetry install -E postgres # Or use -E mysql
```

5. Setup your configuration and save it in `config.json`. The provided `config.json.example` has the skeleton of what you need to provide: source and destination database connection details, as well as subsetting goals in `initial_targets`. Here's an example that will collect 10% of a table named `public.target_table`.
```
"initial_targets": [
    {
        "table": "public.target_table",
        "percent": 10
    }
]
```
There may be more required configuration depending on your database, but simple databases should be easy. See the Config section for more details, and `config.json.example_all` for all of the options in a single config file.

5. Run! `$ poetry run subset`

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

`upstream_filters`: Additional filtering to be applied to tables during upstream subsetting. Upstream subsetting happens when a row is imported, and there are rows with foreign keys to that row. The subsetter then greedily grabs as many rows from the database as it can, based on the rows already imported. If you don't want such greedy behavior you can impose additional filters with this option. This is an advanced feature, you probably won't need for your first subsets. The value is a JSON array of JSON objects. See `example-config.json` for details.

`fk_augmentation`: Additional foreign keys that, while not represented as constraints in the database, are logically present in the data. Foreign keys listed in `fk_augmentation` are unioned with the foreign keys provided by constraints in the database. `fk_augmentation` is useful when there are foreign keys existing in the data, but not represented in the database. The value is a JSON array of JSON objects. See `example-config.json` for details.

`dependency_breaks`: An array containing JSON objects with *"fk_table"* and *"target_table"* fields of table relationships to be ignored in order to break cycles

`keep_disconnected_tables`: If `true` tables that the subset target(s) don't reach, when following foreign keys, will be copied 100% over. If it's `false` then their schema will be copied but the table contents will be empty. Put more mathematically, the tables and foreign keys create a graph (where tables are nodes and foreign keys are directed edges) disconnected tables are the tables in components that don't contain any targets. This setting decides how to import those tables.

`max_rows_per_table`: This is interpreted as a limit on all of the tables to be copied. Useful if you have some very large tables that you want a sampling from. For an unlimited dataset (recommended) set this parameter to `ALL`.

`pre_constraint_sql`: An array of SQL commands that will be issued on the destination database after subsetting is complete, but before the database constraints have been applied. Useful to perform tasks that will clean up any data that would otherwise violate the database constraints. `post_subset_sql` is the preferred option for any general purpose queries.

`post_subset_sql`: An array of SQL commands that will be issued on the destination database after subsetting is complete, and after the database constraints have been applied. Useful to perform additional adhoc tasks after subsetting.

# Running

Almost all the configuration is in the `config.json` file, so running is as simple as

```
$ poetry run subset
```

Two commandline arguements are supported:

`-v`: Verbose output. Useful for performance debugging. Lists almost every query made, and it's speed.

`--no-constraints`: For Postgres this will not add constraints found in the source database to the destination database. This option has no effect for MySQL.
