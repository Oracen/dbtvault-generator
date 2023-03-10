# dbtvault-generator
Generate DBT Vault files from yml metadata!

## Why this exists
A good dev is a lazy dev. Let's do less work, the right way!

DBTVault is a great tool, but due to quirks of DBT (looking at you, compile context) we can't always have access to the variables we want to use. This tool is a workaround to allow data vault to be generated entirely from yaml files. This means that the source-of-truth now resides in an easily-parseable format, instead of relying on post-facto parsing of SQL (or the information schema) to determine what outputs to expect.

## Requirements
- `python>=3.9`
- `dbt>=1.0.0` available in the Python environment
- A DBT project initialised and ready to go. The `dbtv-gen` CLI tool needs to execute from the same directory as you would normally execute DBT from.
- The DBT package `dbtvault>0.9.0` installed in the DBT project

## Overview Of Features
Depending on your set-up, install `dbtvault-generator` into your Python environment. A shim will be installed that adds `dbtv-gen` to your CLI, and you're ready to go.

The following assumes you have `dbtvault.yml` files preconfigured (see below for details).

![image](./static/images/folder-setup.png)

The `dbtvault.yml` file contains the metadata required for templating the `dbtvault` library

![image](./static/images/dbtvault-yml.png)

You can also use a root-level `dbtvault.yml` to specify global defaults.

![image](./static/images/root-dbtvault.png)

### Generate SQL Templates
In your command line of choice, run the following command:
```bash
dbtv-gen sql
```
By default, you will see the SQL files appear in the same directory as the `dbtvault.yml` file. Files can be prefixed automatically (shown), can be manually specified per-model, or ignored.
![image](./static/images/sql-files-created.png)

The contents of the file will match the details specified in `dbtvault.yml`.

![image](./static/images/sql-file-details.png)


### Generate `schema.yml` Doc Config

Once the sql has been generated, execute `dbt run` to instantiate them as views in your development environment. From here, `dbt docs generate` can be executed to build DBT's `catalog.json` artifact. `dbtvault-generator` can then use this to pre-populate a `schema.yml` file with basic documentation.

```bash
dbtv-gen docs
```

![image](./static/images/schema-file-created.png)


The schema file will infer certain properties about the columns. This includes the data types of the columns, read from the information schema. Unfortunately we can't yet describe the columns automatically.

![image](./static/images/schema-file-details.png)

In additionFor example, primary keys will automatically have `not_null` and `unique` tests added for alerting of clashes. Foreign key columns will have a `relationships` test added, but this test is conditional on a `where 1 != 1` condition to prevent it from triggering alerts. What this does do, however, is play nicely with other tools and packages that use the "relationship" test to automatically create foreign keys or similar metadata (e.g. on Snowflake) for downstream use by other tools.


Instantiating the models is currently required to build the docs. In future versions, the docs will be built directly from the config options.

### BONUS! Use `dbterd` To Generate ER Diagrams!

The wonderful [Dat Nguyen](https://github.com/datnguye) has built [a cool library called `dbterd`](https://github.com/datnguye/dbterd) that picks up catalog objects and, using the relationship test, creates a `.dbml` file detailing the core relationships within your DBT catalog. The `.dbml` format is an open-source entity-relation spec that can either be consumed directly by a number of SaaS tools or by open-source tools like [DBML Renderer](https://github.com/softwaretechnik-berlin/dbml-renderer). Importantly, `dbterd` uses the `relationships` test to identify foreign key relations...which you will have out of the box.

Go check out his page on Github and leave him a star while you're there.

## Installation Instructions

Pip coming soon

### Direct From Git
Many package managers allow installation of Python packages direct from version control repositories. Add this repo directly

Version tagging coming soon.

### From Source
For now, using PDM as dependency manager as it seems to be better supported than Poetry. As such, give PDM a try! It's like Yarn for Python, or Poetry with a faster resolver and Poe built in.

```bash
git clone https://github.com/Oracen/dbtvault-generator
cd dbtvault-generator
pdm build
```
