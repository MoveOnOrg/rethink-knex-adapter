rethinkRowFuncs = {
  new: function(query, rowname) {
    this.query = query
    this.rowname = rowname
  },
  eq: function(eqVal) {
  }
}


function rethinkQuery(kninky) {
    this.kninky = kninky
}

rethinkQuery.prototype = {
  changes: function() {
    return this
  },

  delete: function() {
    // deletes results of query
    return this
  },

  eqJoin: function(tableField, rTableResult, isRightJoin) {
    //when result is 'run' with ('right') it's a right join?!!
    return this
  },

  filter: function(func_or_dict) {
    // when a dict, it functions like a regular WHERE filter
    // when a function, it needs to filter post-query (sad?)
    return this
  },

  find: function(func) {
    return this
  },

  //forEach(func) {} -- not actually a query -- only on arrays
  get: function(pk_val) {  // returns single result
    return this
  },

  getAll: function(values, index_dict) {
    //@values can be a single value or an array of values for unique pairs
    //@index_dict will be in the form {index: INDEX_NAME}
    return this
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
    return this
  },

  pluck: function(fieldName) {
    return this
  },

  table: function(table_name) {
    return this
  },

  then: function(func) {
  },

  //STATIC METHODS (used directly rather than on a query builder)
  and: function() {
  },

  desc: function() {
  },

  not: function() {
  },

  row: function(rowname) {
    var rowfunc = function(col) {
      rowfunc.col = col
    }
    rethinkRowFuncs.new.call(rowfunc, this, rowname)
    Object.assign(rowfunc, rethinkRowFuncs)
    return rowfunc
  }
}

module.exports = rethinkQuery
