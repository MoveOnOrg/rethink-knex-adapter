function rethinkQuery(kninky) {
    this.kninky = kninky
}

rethinkQuery.prototype = {
  delete: function() {
    // deletes results of query
  },

  eqJoin: function(tableField, rTableResult, isRightJoin) {
    //when result is 'run' with ('right') it's a right join?!!
  },

  filter: function(func_or_dict) {
    // when a dict, it functions like a regular WHERE filter
    // when a function, it needs to filter post-query (sad?)
  },

  find: function(func) {
  },

  //forEach(func) {} -- not actually a query -- only on arrays
  get: function(pk_val) {  // returns single result
  },

  getAll: function(values, index_dict) {
    //@values can be a single value or an array of values for unique pairs
    //@index_dict will be in the form {index: INDEX_NAME}
  },

  now: function() {
    var val = {timestamp: 'now',
               dt: new Date(),
               sub: function(secs) {
                 val.dt = new Date(val.dt - secs * 1000)
                 return val
               }
              }
    return val
  },

  orderBy: function(desc_res) {
  },

  pluck: function(fieldName) {
  },

  table: function(table_name) {
  },

  //STATIC METHODS (used directly rather than on a query builder)
  desc: function() {
  },

  not: function() {
  }
}

module.exports = rethinkQuery
