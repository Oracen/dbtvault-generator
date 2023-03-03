# dbtvault-generator
Generate DBT Vault files from yml metadata!

## Why this exists
A good dev is a lazy dev. Let's do less work, the right way!

DBTVault is a great tool, but due to quirks of DBT (looking at you, compile context) we can't always have access to the variables we want to use. This tool is a workaround to allow data vault to be generated entirely from yaml files. This means that the source-of-truth now resides in an easily-parseable format, instead of relying on post-facto parsing of SQL (or the information schema) to determine what outputs to expect.


## Installation Instructions

For now, using PDM as dependency manager as it seems to be better supported than Poetry. As such, give PDM a try! It's like Yarn for Python, or Poetry with a faster resolver and Poe built in.
