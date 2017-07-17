var Promise = require('bluebird');
var Term = require('rethinkdbdash/lib/term.js')
var Rethink = require('rethinkdbdash')

Term.prototype.XXexpr = function(expression, nestingLevel) {
  console.log('NEWTERM', expression, nestingLevel, this._query)
  return this
}


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
  var term = function(field) {
    return term.bracket(field)
  }
  //magic so the object is callable
  term.__proto__ = this.__proto__
}

rethinkQuery.prototype = {
  bracket: function() {
    
  },

  changes: function() {
    return this
  },

  count: function() {
    return this
  },

  default: function(defaultVal) {
    return this.resultVal || defaultVal
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
    if (typeof func_or_dict == 'function') {
      // MORETODO
      //then()
    } else if (func_or_dict) {
      this.knexQuery = this.knexQuery.where(func_or_dict)
    }
    return this
  },

  find: function(func) {
    return this
  },

  //forEach(func) {} -- not actually a query -- only on arrays
  get: function(pk_val) {  // returns single result
    return this
  },

  getAll: function() {
    //@values can be a single value or an array of values for unique pairs
    //@index_dict will be in the form {index: INDEX_NAME}
    // TODO (not!): not going to implement possibility of multiple vals with complex indices
    var model = this.kninky.models[this.tableName]
    var index = [model.pk] //default
    var lastArg = arguments[arguments.length-1]
    var notValArgs = (lastArg.index ? 1 : 0)
    // all but the last arg, if it's the index thingie:
    var valArgs = Array.prototype.slice.call(arguments, 0, arguments.length - notValArgs);
    console.log(lastArg)
    if (lastArg.index
        && lastArg.index != index
        && lastArg.index in model.indexes) {
          index = model.indexes[lastArg.index]
    }

    if (valArgs.length > 1) {
      // do whereIn here
      this.knexQuery = this.knexQuery.whereIn(index[0], valArgs)
    } else if (valArgs.length > 0) {
      var queryDict = {}
      index.forEach(function(ind, i) {
        queryDict[ind] = valArgs[i]
      })
      console.log('queryDict', queryDict)
      this.knexQuery = this.knexQuery.where(queryDict)
    }
    return this
  },

  group: function() {
    return this
  },

  limit: function() {
    var self = this
    return function(asking_for_index) {
      return self
    }
  },

  orderBy: function(desc_res) {
    return this
  },

  pluck: function(fieldName) {
    /*MORETODO: 
      I think this needs to 
     */
    return function(return_column) {
    }
  },

  sum: function() {
    return this
  },

  table: function(tableName) {
    var newQuery = new rethinkQuery(this.kninky)
    newQuery.tableName = tableName
    newQuery.knexQuery = this.kninky.k.from(tableName)
    return newQuery
  },

  then: function(func, catchfunc) {
    console.log('running then()')
    if (this.knexQuery) {
      //MORETODO: select() is only if we are selecting/ vs updating/etc
      return //this.knexQuery.select().then(func, catchfunc)
      return this.knexQuery.select().then(function(x) {
        console.log('knex result', x)
        return []
      })
    } else {
      var p = new Promise().then(func, catchfunc)
      p.resolve([])
    }
  },

  ungroup: function() {
    return this
  }
}

function staticR(kninky) {
  console.log('STATICR', this)
  this.kninky = kninky
  return this
  var _r = function(x) {
    return new Term(_r).expr(x)
  }
  //_r.__proto__ = this.__proto__
  //_r.row = new Term(_r).row()
  _r._Term = Term
  console.log('PROTOTYPE', this.prototype)
  return _r
}

var _r = Rethink()
staticR.prototype = {
  nextVarId: 1,
  xxtable: function(table, options) {
    return new Term(this).table(table, options)
  },
  table: _r.__proto__.table,

  getPoolMaster: function() {
    return {
      getConnection: function() {
        return Promise.resolve({
          _getToken: function(){},
          db: {notnull:true},
          emit: function(){console.log('emit')},
          _send: function(query, token, resolve, reject, originalQuery, options, end) {
            console.log('SEND', JSON.stringify(query))
            console.log('SEND orig', JSON.stringify(originalQuery), options, token)
          },
          _isConnection: function() {return true},
          _isOpen: function() {return true}
        })
      }
    }
  },

  //STATIC METHODS (used directly rather than on a query builder)
  and: _r.__proto__.and,
  db: _r.__proto__.db,
  desc: _r.__proto__.desc,
  expr: _r.__proto__.expr,
  not: _r.__proto__.not,
  now: _r.__proto__.now,
  xxnow: function() {
    //MORETODO: need to get new createModel or model.js to
    // use whatever now() the original creates
    var val = {timestamp: 'now',
               dt: new Date(),
               sub: function(secs) {
                 val.dt = new Date(val.dt - secs * 1000)
                 return val
               }
              }
    return val
  },
  row: _r.row,
  //MORETODO: need to get row for createModel or model.js to
  xxrow: function(rowname) {
    //r.row('new_val')('is_from_contact'), r.row('old_val').eq(null))
    var rowfunc = function(col) {
      rowfunc.col = col
    }
    rethinkRowFuncs.new.call(rowfunc, this, rowname)
    Object.assign(rowfunc, rethinkRowFuncs)
    return rowfunc
  }
}
//console.log('PROTOTYPE RETHINK', _r.__proto__)

module.exports = staticR //rethinkQuery
