---
displayed_sidebar: docs
---

# SHOW DATA

SHOW DATA is used to display the amount of data, the number of copies, and the number of statistical rows in a database or a database table.

Syntax:

```sql
SHOW DATA [FROM [<db_name>.]<table_name>]
```

Note:

1. If the FROM clause is not specified, the amount of data and copies subdivided into each table in the current db will be displayed. Where the data volume is the total data volume of all replicas. The number of copies is the number of copies of all partitions of the table and all materialized views.
2. If the FROM clause is specified, the amount of data, the number of copies and the number of statistical rows subdivided into each materialized view under the table are displayed. Where the data volume is the total data volume of all replicas. The number of copies is the number of copies of all partitions corresponding to the materialized view. The number of statistical rows is the number of statistical rows of all partitions corresponding to the materialized view.
3. When counting the number of rows, the copy with the largest number of rows among multiple copies shall prevail.
4. The Total row in the result set represents the summary row. The Quota row represents the quota set by the current database. The Left line represents the remaining quota.

## Examples

Example 1: Display the data volume, copy quantity, summary data volume and summary copy quantity of each table in the current database.

```plain text
SHOW DATA;
+-----------+-------------+--------------+
| TableName | Size        | ReplicaCount |
+-----------+-------------+--------------+
| tbl1      | 900.000 B   | 6            |
| tbl2      | 500.000 B   | 3            |
| Total     | 1.400 KB    | 9            |
| Quota     | 1024.000 GB | 1073741824   |
| Left      | 1021.921 GB | 1073741815   |
+-----------+-------------+--------------+
```

Example 2: Display the amount of breakdown data, the number of copies, and the number of statistical rows in the specified table.

```plain text
SHOW DATA FROM example_db.test;
+-----------+-----------+-----------+--------------+----------+
| TableName | IndexName | Size      | ReplicaCount | RowCount |
+-----------+-----------+-----------+--------------+----------+
| test      | r1        | 10.000MB  | 30           | 10000    |
|           | r2        | 20.000MB  | 30           | 20000    |
|           | test2     | 50.000MB  | 30           | 50000    |
|           | Total     | 80.000    | 90           |          |
+-----------+-----------+-----------+--------------+----------+
```
