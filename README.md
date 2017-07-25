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
