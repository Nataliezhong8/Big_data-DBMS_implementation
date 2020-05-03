## Catalog
### 1.pg_database
A logical scope that collects together a number of schemas
```
#\d pg_database
```
### 2.pg_namespace
A logical scope used as a namespace; contains a collection of database objects (tables, views, functions, indexes, triggers, ...).
```
#\d pg_namespace
```
### 3. pg_tablespace
A physical scope identifying a region of the host filesystem where PostgreSQL data files are stored.
```
#\d pg_tablespace
```

## psql command
1. Generates a list of all databases in your cluster: 
```
$ psql -l
```
or
```
# \l --in db
```
2. Connects to the database db and reads commands from the file called file to act on that database
```
$ psql db -f file
```
or
```
# \i file
```
3.  turns on a timer that indicates how long each SQL command takes to execute.
```
# \timing
```
