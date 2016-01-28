# Calypso Protocol

> Being an artist when designing interfaces !

+ Simple.
+ Easy to learn, to use, to extend and to maintain.

## Field
### AS
### Function

## Table References
```
table_references:
    escaped_table_reference [, escaped_table_reference] ...

escaped_table_reference:
    table_reference
  | { OJ table_reference }

table_reference:
    table_factor
  | join_table

table_factor:
    tbl_name [[AS] alias] [index_hint_list]
  | table_subquery [AS] alias
  | ( table_references )                  # unsupported

join_table:
    table_reference [INNER | CROSS] JOIN table_factor [join_condition]
  | table_reference STRAIGHT_JOIN table_factor
  | table_reference STRAIGHT_JOIN table_factor ON conditional_expr
  | table_reference {LEFT|RIGHT} [OUTER] JOIN table_reference join_condition
  | table_reference NATURAL [{LEFT|RIGHT} [OUTER]] JOIN table_factor
```

| name          | sample | description |
| ---           |  ---:  | --- |
| Table/Table   |        | [class] | 
| Join          | *refer to join for more* | [class] |
| CompoundTable | `CompoundTable(table_l, table_r, ...) [*args]` | [class] |

### Table _single_

### Join
https://dev.mysql.com/doc/refman/5.5/en/join.html

MySQL supports the following `JOIN` syntax for the table_references part of
`SELECT` statements and multiple-table `DELETE` and `UPDATE` statements:
```
table_references:
    escaped_table_reference [, escaped_table_reference] ...

escaped_table_reference:
    table_reference
  | { OJ table_reference }

table_reference:
    table_factor
  | join_table

table_factor:
    tbl_name [[AS] alias] [index_hint_list]
  | table_subquery [AS] alias
  | ( table_references )

join_table:
    table_reference [INNER | CROSS] JOIN table_factor [join_condition]       # disallow join self
  | table_reference STRAIGHT_JOIN table_factor                               # disallow join self
  | table_reference STRAIGHT_JOIN table_factor ON conditional_expr           # disallow join self
  | table_reference {LEFT|RIGHT} [OUTER] JOIN table_reference join_condition # allow join self
  | table_reference NATURAL [{LEFT|RIGHT} [OUTER]] JOIN table_factor         # disallow join self

join_condition:
    ON conditional_expr
  | USING (column_list)

index_hint_list:
    index_hint [, index_hint] ...

index_hint:
    USE {INDEX|KEY}
      [FOR {JOIN|ORDER BY|GROUP BY}] ([index_list])
  | IGNORE {INDEX|KEY}
      [FOR {JOIN|ORDER BY|GROUP BY}] (index_list)
  | FORCE {INDEX|KEY}
      [FOR {JOIN|ORDER BY|GROUP BY}] (index_list)

index_list:
    index_name [, index_name] ...
```

According to mysql documents, it's recommended to use `ON` for column match and
`SELECT WHERE` to shrink the search results.

#### Protocol
```
t1.join( t2, mode=X ).on( condition )   # lower-case [join]
Join( t1, t2, mode=X ).on( condition )
Join( t1, t2, mode=X ).using( column_list )
```

**mode** | **keyword** | **condition** | **allow join self** | **description**
| --- | ---: | ---: | :---: | :---: |
|`INNER` | `INNER JOIN` | join_condition | `N` |
|`CROSS` | `CROSS JOIN` | join_condition | `N` |
|`STRAIGHT` | `STRAIGHT_JOIN` | condition_expr | `N` | conditional_expr is optional
|`LEFT` (outer) | `LEFT [OUTER] JOIN` | join_condition | `Y` |
|`RIGHT` (outer) | `RIGHT [OUTER] JOIN` | join_condition | `Y` |
|`NATURAL` | | | | ignore this mode|

Notice, on different mode, the condition may differs (`ON` or `USING`)


_SQL example_
```
SELECT
cpa.category_id, cpa.property_type, cpa.property_id, p.name, p.cn_name,
p.access_type, p.value_type, p.description

FROM
property AS p
JOIN (category_property_association AS cpa ) ON (cpa.property_id = p.id)
JOIN (property_visual_level AS pvl ) ON (pvl.property_id = p.id)

WHERE
(cpa.category_id = 3 AND pvl.visual_level = 'user’);
```

### Leak Fields
list of fields names, or functions.

#### Protocol
```
Function( field ).AS('xxx')    # [class]

Max( field )
Min( field )
...
```

### Table AS
```
# AS only enable for such case:

"SELECT * FROM table AS f JOIN table AS c ON f.father_id = c.id WHERE f.id > 0 AND c.id < 100"

f, c= t.AS('f'), t.AS('c')
j = Join(f, c)                       # table AS f/c
j.on(f.father_id == c.id)            # f/c
j.select().where( f.id > 0 & c.id < 100).execute()    # f/c
```

### CompoundTable

### Order By

### Limit




## Insert
https://dev.mysql.com/doc/refman/5.5/en/insert.html
### Sql Syntax
```
INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
    [INTO] tbl_name [(col_name,...)]
    {VALUES | VALUE} ({expr | DEFAULT},...),(...),...
    [ ON DUPLICATE KEY UPDATE
      col_name=expr
        [, col_name=expr] ... ]
```
_Or_
```
INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
    [INTO] tbl_name
    SET col_name={expr | DEFAULT}, ...
    [ ON DUPLICATE KEY UPDATE
      col_name=expr
        [, col_name=expr] ... ]
```
_Or_
```
INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE]
    [INTO] tbl_name [(col_name,...)]
    SELECT ...
    [ ON DUPLICATE KEY UPDATE
      col_name=expr
        [, col_name=expr] ... ]
```

### Insert Mode
+ `INST_MODE_IGNORE`: ignore mode
+ `INST_MODE_UPDATE`: on_duplicate_update mode - *USE WITH CAUTION*
+ `INST_MODE_REPLACE`: replace mode - *NON-STANDARD*

### Protocol
```
model = Model()
model.a = 'a'
model.b = 'b'
model.c = 'c'

table.insert( model )
```
_Or_
```
dict_data = { table.a:'a', table.b:'b', table.c:'c' }
dict_data = { 'a':'a', 'b':'b', 'c':'c' }

table.insert( dict_data )
table.insert( [dict_data, ...] )  # batch insertion
```

## Delete
https://dev.mysql.com/doc/refman/5.5/en/delete.html
### Sql Syntax
```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```
_Or_
```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
    tbl_name[.*] [, tbl_name[.*]] ...
    FROM table_references
    [WHERE where_condition]
```
_Or_
```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
    FROM tbl_name[.*] [, tbl_name[.*]] ...
    USING table_references
    [WHERE where_condition]
```

### Protocol
```
cond = xxx
table.delete( cond )
```

## Update
https://dev.mysql.com/doc/refman/5.5/en/update.html
### Sql Syntax
```
UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```
_Or_
```
UPDATE [LOW_PRIORITY] [IGNORE] table_references
    SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
    [WHERE where_condition]
```

### Protocol
```
table.a = 'a';
table.b = 'b';
table.c = 'c';

table.update( cond=None )
```
_Or_
```
dict_data = { table.a:'a', table.b:'b', table.c:'c' }
dict_data = { 'a':'a', 'b':'b', 'c':'c' }

table.update( dict_data, cond=None )
```

## Select
https://dev.mysql.com/doc/refman/5.5/en/select.html

### Sub Query
> same as select.

### Sql Syntax
```
SELECT
    [ALL | DISTINCT | DISTINCTROW ]
      [HIGH_PRIORITY]
      [STRAIGHT_JOIN]
      [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
      [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
    select_expr [, select_expr ...]

    [FROM table_references

    [WHERE where_condition]

    [GROUP BY {col_name | expr | position}
      [ASC | DESC], ... [WITH ROLLUP]]

    [HAVING where_condition]

    [ORDER BY {col_name | expr | position}

      [ASC | DESC], ...]

    [LIMIT {[offset,] row_count | row_count OFFSET offset}]

    [PROCEDURE procedure_name( argument_list )]

    [INTO OUTFILE 'file_name' export_options
      | INTO DUMPFILE 'file_name'
      | INTO var_name [, var_name]]

    [FOR UPDATE | LOCK IN SHARE MODE]]
```

### Protocol
```
select   ( leak_fields )
from     ( table_references )
where    ( cond )
group by ( field )
order by ( field )
limit    ( row_count, offset )

1. table.select( leak_fields=[X] ).where( cond ).groupBy( X ).orderBy( X ).limit( X ).execute()
2. table.select( leak_fields=[X], cond=X, groupBy=X, orderBy=X, limit=X ).execute()
```
_Or_
```
table.select( leak_fields=[X] )...

mlti_tbl = Join( ... ) / CompoundTable( ... )
mlti_tbl.select( leak_fields=[X] )...
```

### Condition [class]

| **keyword**  | **sample** | **type** | **description** |
| :----------- | ----: | -----: | -----:|
| `AND`        | `Field_b & Field_b` | `CONJ` | binary conj |
| `OR`         | <code>Field_a &#124; Field_b</code> | `CONJ` | binary conj |
| `NOT`        | `Not( Field )` | `CONJ` | unary conj |
| `LIKE`       | `Like( Field, stuff )` | `ATOM` | opposite: `Not( Like( t.f, stuff ) )` |
| `IN`         | `In( Field, value_list )` | `ATOM`| opposite: `Not( In( ... ) )` |
| `>`          | `>` | `ATOM` | |
| `<`          | `<` | `ATOM` | |
| `!=` or `<>` | `!=` | `ATOM`| |

### Protocol
```
cond = Field ATOM value
cond = cond CONJ
cond = cond CONJ cond
```

Example
```
cond = ( t1.a == 1 & ( t1.b == 2 | Not(t2.c != 3) ) )
       & Not( In(t3.d , [1,2,3]) )
       | Not( Like( t4.f, 'stuff' ) )
```
