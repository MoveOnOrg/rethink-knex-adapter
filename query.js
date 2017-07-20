var Promise = require('bluebird');
var Term = require('rethinkdbdash/lib/term.js')
var ProtoDef = require('rethinkdbdash/lib/protodef.js')
var Rethink = require('rethinkdbdash')
var Document = require('./models').Document

rethinkRowFuncs = {
  new: function(query, rowname) {
    this.query = query
    this.rowname = rowname
  },
  eq: function(eqVal) {
  }
}

var reverseTerms = {}
var reverseDatum = {}
var noProcess = {
  'FUNC': 1,
  'FUNCALL': 1
}
for (var a in ProtoDef.Term.TermType) {
  reverseTerms[ProtoDef.Term.TermType[a]] = a
}
for (var b in ProtoDef.Datum.DatumType) {
  reverseDatum[ProtoDef.Datum.DatumType[b]] = b
}


function rethinkQuery(kninky, query) {
  this.kninky = kninky
  this.flattened = this.flattenQuery(query)
  this.brackets = []
  
}

rethinkQuery.prototype = {
  flattenQuery: function(query) {
    // recursive
    // Takes the 'machine' json sent to a rethink server and flattens
    // it 'back' to sql-ish commands.  We do this rather than re-implementing
    // rethinkdbdash's term.js to avoid crazy object parsing.
    // Serialized entities are way-easier to process.
    var flattened = []
    if (query[0] == 1) {
      //FUTURE: query[2] will be the db info, if useful/necessary
      flattened = this.flattenQuery(query[1])
      flattened.reverse()
    } else {
      var cmd = reverseTerms[query[0]] || query[0]
      var args = [] //args of this command
      flattened.push([cmd, args]) //args is filled in by-ref below
      if (query.length > 1) {
        if (Array.isArray(query[1])) {
          var firstwrap = query[1];
          var arg1ind = 1
          if (Array.isArray(firstwrap) && !noProcess[cmd]) {
            arg1ind = 2
            //TODO: this can probably be cleaner, but works
            for (var i=0,l=firstwrap.length; i<l; i++)  {
              var secondwrap = firstwrap[i]
              if (i==0 && Array.isArray(secondwrap)) {
                var moreCommands = this.flattenQuery(secondwrap)
                flattened.push.apply(flattened, moreCommands)
              } else {
                if (Array.isArray(secondwrap)
                    && secondwrap.length == 2
                    && secondwrap[0] == 2) {
                  //MAKE_ARRAY has code 2,
                  // so this is how we have an array as an argument
                  args.push(secondwrap[1])
                } else { //normal
                  args.push(secondwrap)
                }
              }
            }
          }
          args.push.apply(args, query.slice(arg1ind))
        }
      }
    }
    return flattened
  },
  processQuery: function(flattened) {
    //after this.flattened is set, this will actually run it through knex
    flattened = flattened || this.flattened
    for (var i=0,l=flattened.length; i<l; i++) {
      var f = flattened[i]
      var cmd = f[0]
      var args = f[1]
      if (cmd in this) {
        this[cmd].apply(this, args)
      } else {
        console.log('MISSING IMPLEMENTATION', cmd)
      }
    }
  },

  then: function(func, catchfunc) {
    var self = this
    console.log('running then()')
    if (this.isChangesListener) {
      //TODO: setup a ?local listener for save changes
      console.log('UNIMPLEMENTED: CHANGES LISTENER')
    } else if (this.knexQuery) {
      console.log('KNEX QUERY', this.knexQuery._method)
      if (self.currentJoin && !self.currentJoin.select) {
        // we need to structure the output as {'left': {...}, 'right': {....}}
        // so we need to make the fields distinguishable to separate them later
        var columns = []
        var lTable = self.currentJoin.left
        var rTable = self.currentJoin.right
        var lModel = this.kninky.models[lTable]
        var rModel = this.kninky.models[rTable]
        for (var lField in lModel.fields) {
          columns.push(lTable + '.' + lField + ' as left_' +lField)
        }
        for (var rField in rModel.fields) {
          columns.push(rTable + '.' + rField + ' as right_' +rField)
        }
        this.knexQuery = this.knexQuery.select(columns)
      }

      return this.knexQuery.then(function(x) {
        //TODO: need to ?sometimes? turn knex results into model objects
        console.log('knex result b', self.brackets)
        console.log('knex result', x)
        if (self.currentJoin && !self.currentJoin.select) {
          x = x.map(function(res) {
            var newObj = {'left':{}, 'right': {}}
            for (var k in res) {
              if (k.startsWith('left_')) {
                newObj.left[k.slice(5)] = res[k]
              } else if (k.startsWith('right_')) {
                newObj.right[k.slice(6)] = res[k]
              } else {
                newObj[k] = res[k]
              }
            }
          })
        }
        if (self.brackets.length) {
          //MORETODO?: brackets need to be laced with the right parts
          // and then maybe iterated through
          x = x[self.brackets.pop()]
        }
        if (!x && self.defaultVal) {
          x = self.defaultVal
        }
        if (Array.isArray(x)) {
          var model = self.kninky.models[self.tableName]
          x = x.map(function(res) {
            res.__proto__ = new Document(model, model._options)
            return res
          })
        }
        func(x)
      }, catchfunc)
    } else if (func) {
      func([])
    }
  },

  BRACKET: function(index) {
    // see then: implementation for how we 'catch' this
    if (this.currentJoin && (index == 'right' || index == 'left')) {
      // we need to restrict the results to just the columns from
      // one of the join sides
      this.currentJoin['select'] = index
      var columns = []
      var selectTable = this.currentJoin[index]
      var selectModel = this.kninky.models[selectTable]
      for (var sField in selectModel.fields) {
        columns.push(selectTable + '.' + sField)
      }
      this.knexQuery = this.knexQuery.select(columns)
    } else if (this.currentPluck == index) {
      // do nothing: this is knex's default behavior:
      //  pluck('foo') => ['foo_value1', 'foo_value2'...]
      // whereas rethinkdb defaults to
      //  pluck('foo') => [{'foo': 'foo_value1'}, {'foo': foo_value2'},...]
      // but r.pluck('foo')('foo') is so common, we'll just ASSUME it
    } else {
      this.brackets.push(index)
    }
  },

  CHANGES: function() {
    this.isChangesListener = true
  },

  COUNT: function() {
    if (this.knexQuery) {
      this.knexQuery = this.knexQuery.count().then(function(countResult) {
        return Number(countResult[0]['count(*)'])
      })
    }
  },

  DEFAULT: function(defaultVal) {
    this.defaultVal = defaultVal
  },

  DELETE: function() {
    if (this.knexQuery) {
      this.knexQuery = this.knexQuery.delete()
    }
  },

  DISTINCT: function() {
    if (this.knexQuery) {
      this.knexQuery = this.knexQuery.distinct()
    }
  },

  EQ_JOIN: function(leftTableField, rTableResult, rightTableIndex) {
    //when result is 'run' with ('right') it's a right join?!!
    /*
      still todo:
      - output rows as 'left': ... and 'right'...
      - .. except when a BRACKET('right'/'left') is sent
        and then only select those
      - support filter((row) => (
           row('right').... (or left) for mapping
     */
    var rightTableName
    if (Array.isArray(rTableResult) && rTableResult[0] == 15) {
      // example: [ 15, [ 'organization' ] ]
      rightTableName = rTableResult[1][0]
    } else {
      console.error("right join table of unknown type", rTableResult)
      throw new Error('right join table of unknown type')
    }
    this.currentJoin = {
      'left': this.tableName,
      'right': rightTableName
    }
    var rightModel = this.kninky.models[rightTableName]
    var rightTableField = ((rightTableIndex && rightTableIndex.index)
                           ? rightTableIndex.index
                           : rightModel.pk)
    console.log('right model', rTableResult)
    this.knexQuery = this.knexQuery.join(
      rightTableName,
      this.tableName + '.' + leftTableField,
      rightTableName + '.' + rightTableField
    )
  },

  FILTER: function(func_or_dict) {
    // when a dict, it functions like a regular WHERE filter
    // when a function, it needs to filter post-query (sad?)
    if (Array.isArray(func_or_dict)) {
      //need to process the function as byte-code-ish stuff
      // MORETODO
      //then()
    } else if (func_or_dict) {
      this.knexQuery = this.knexQuery.where(func_or_dict)
    }
  },

  FIND: function(func) {
    console.log('UNIMPLEMENTED FIND')
  },

  GET: function(pk_val) {  // returns single result
    this.knexQuery = this.knexQuery.where(this.pk, pk_val)
  },

  GET_ALL: function() {
    //@values can be a single value or an array of values for unique pairs
    //@index_dict will be in the form {index: INDEX_NAME}
    // TODO (not!): not going to implement possibility of multiple vals with complex indices
    var model = this.kninky.models[this.tableName]
    var index = [model.pk] //default
    var lastArg = arguments[arguments.length-1]
    var notValArgs = (lastArg.index ? 1 : 0)
    // all but the last arg, if it's the index thingie:
    var valArgs = Array.prototype.slice.call(arguments, 0, arguments.length - notValArgs);
    console.log('valargs', valArgs, lastArg)
    if (lastArg.index
        && lastArg.index != model.pk
        && lastArg.index in model.indexes
       ) {
      index = model.indexes[lastArg.index]
    }

    if (valArgs.length > 1) {
      // do whereIn here
      this.knexQuery = this.knexQuery.whereIn(index[0], valArgs)
    } else if (valArgs.length > 0) {
      var queryDict = {}
      index.forEach(function(ind, i) {
        if (Array.isArray(valArgs[0])) {
          //for multi-field indexes
          queryDict[ind] = valArgs[0][i]
        } else {
          queryDict[ind] = valArgs[i]
        }
      })
      console.log('queryDict', queryDict)
      this.knexQuery = this.knexQuery.where(queryDict)
    }

  },

  GROUP: function() {
    //TODO
    console.error('UNIMPLEMENTED GROUP')
  },

  LIMIT: function(max) {
    if (this.knexQuery) {
      this.knexQuery = this.knexQuery.limit(max)
    }
  },

  MERGE: function(func) {
    //TODO
    console.error('UNIMPLEMENTED MERGE ')
  },

  ORDER_BY: function(desc_res) {
    if (this.knexQuery) {
      var direction = 'asc'
      var key = desc_res
      if (Array.isArray(desc_res) && Array.isArray(desc_res[1])) {
        // this looks something like: [74,["due_by"]]
        // 74 is desc, 73 is asc
        key = desc_res[1][0]
        direction = reverseTerms[desc_res[0]].toLowerCase()
      } else if (typeof desc_res == 'function') {
        //FUTURE: dumb feature, let's not implement
        return
      }
      console.log('orderBy', key, direction)
      this.knexQuery = this.knexQuery.orderBy(key, direction)
    }
  },

  PLUCK: function(fieldName) {
    if (this.knexQuery) {
      this.knexQuery = this.knexQuery.pluck(fieldName)
    }
    this.currentPluck = fieldName // for bracket, it's redundant
  },

  SUM: function() {

  },

  TABLE: function(tableName) {
    this.tableName = tableName
    this.knexQuery = this.kninky.k.from(tableName)
    return this.knexQuery
  },

  UNGROUP: function() {

  },

  UPDATE: function(updateData) {
    this.knexQuery = this.knexQuery.update(updateData)
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
  nextVarId: 0, //this is used to index variable references in queries
  xxtable: function(table, options) {
    return new Term(this).table(table, options)
  },
  table: _r.__proto__.table,

  getPoolMaster: function() {
    var self = this
    return {
      getConnection: function() {
        return Promise.resolve({
          _getToken: function(){},
          db: {notnull:true},
          emit: function(){console.log('emit')},
          _send: function(query, token, resolve, reject, originalQuery, options, end) {
            var r2kQuery = new rethinkQuery(self.kninky, query)
            console.log('SEND', JSON.stringify(query))
            console.log('SEND flat', JSON.stringify(r2kQuery.flattened))
            r2kQuery.processQuery()
            r2kQuery.then(resolve, reject)
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
  //row is weird
  row: _r.row,
}
//console.log('PROTOTYPE RETHINK', _r.__proto__)

module.exports = staticR //rethinkQuery
