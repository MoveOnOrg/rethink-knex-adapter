# RethinkDb / Thinky adapter to Knex.js

Rethinkdb is dead.  How do we migrate to a real database?  Let's adapt the Thinky API
to knex so we don't have to rewrite every single query/model definition, at least to
start with.

This project tries to implement a good portion of the Thinky api through Knex, but also assumes you are converting your code 'one way' -- to knex, rather than supporting both

## Configuration/Code Migration

...

## Schema creation

* Since we want real references instead of the weird thing that ReThinkdb does,
  any field ending in `{table}_id` is assumed to be a foreign key reference to {table}
  * To stop this behavior add `.stopReference()` to the model field definition
  * To force this behavior add `.foreign(tableName)` to the model field definition

* We auto-create an `id` increment() field for each table -- even fields marked primary key
  will get it, but `get()` will use what you mark as a primary key

* If you add 'timestamps' to the third argument of createModel,
  it will add `created_at` and `updated_at` timestamp fields
  (you can also use the 'standard' `r.type.date().default(r.now())` code)

* `type.point()` is not supported -- I suggest you change your schema to separate fields for lat/lng

* `type.array()` and `type.object()` are not supported (`object()` is only ok for createModel's table definition)

## Currently supported query methods

Not all queries on `r....` are supported -- some of them are too much of a pain to implement.
I'll take pull requests!

 - [x] bracket -- e.g. `r.table('foo').getAll(bar, {'index': 'bars'}).limit(1)(0)` (The last `(0)` is the 'bracket')
 - [x] bracket post-join for `left`/`right`, e.g. `...eqJoin(foo, r.table(bar))('right')`
 - [ ] `changes()`
 - [x] `count()`
 - [x] `default(defaultVal)`
 - [x] `delete()`
 - [x] `distinct()` NOTE: at least if it works as a sql query
 - [x] `eqJoin(leftTableField, r.table(rightTable))`
 - [x] `eqJoin(leftTableField, r.table(rightTable), rightTableIndex)`
 - [x] `filter(dictOfQueryValues)`
 - [ ] `filter(function)`
 - [x] `get(pkValue)`
 - [x] `getAll(val, {index: column})` (column defaults to pk if not avail)
 - [x] `getAll(...bunchOfIds, {index: column})`
 - [x] `getAll([val1, val2], {index: multi_column_index})`
 - [ ] `group()`
 - [ ] `innerJoin()`
 - [x] `limit(max)`
 - [?] `map(func)` -- probably not, but might work
 - [x] `map({targetVal: r.row(sourceColumn), ...})`
 - [ ] `merge()`
 - [x] `orderBy(column)`
 - [x] `orderBy(r.desc(column))`
 - [x] `pluck(fieldname_or_index)`
 - [ ] `pluck(function)`
 - [ ] `sum()`
 - [x] `table(table)`
 - [ ] `ungroup`
 - [x] `update(updateData)`
 - [x] `zip()`

