var Promise = require('bluebird');

var rethinkQuery = require('./query')
var models = require('./models')

//import rethinkQuery from './query'
//import {modelType} from './models'

function dumbThinky(knexConfig) {
  // for 'direct' access to knex
  this.config = knexConfig
  this.k = require('knex')(knexConfig)
  this.defaultsUnsupported = knexConfig.defaultsUnsupported

  // implementation of (some of) thinky/rethinkdb driver interfaces
  this.r = new rethinkQuery(this)
  //this.r = new Term(this)
  this.type = models.modelType
  
  // list of models 'created' by createModel()
  this.models = {}
}

dumbThinky.prototype = {
  createTables: function(tableNameList) {
    var self = this
    var nextTable = function(i) {
      return function(xxx) {
        return self.createTable(tableNameList[i])
      }
    }
    var p = self.createTable(tableNameList[0])
    for (var i=1,l=tableNameList.length; i<l; i++) {
      p = p.then(nextTable(i))
    }
    return p
  },
  dropTables: function(tableNameList) {
    // goes in reverse order to avoid dependencies
    var self = this
    var i = tableNameList.length
    var p = Promise.resolve(null)
    var nextTable = function(i) {
      return function(xxx) {
        var tableName = tableNameList[i]
        return self.k.schema.dropTableIfExists(tableName)
      }
    }
    while (i--) {
      p = p.then(nextTable(i))
    }
    return p
  },
  postTableCreation: function(tableName) {
    var model = this.models[tableName]
    for (var key in model.tableDoesNotExistListeners) {
      Promise.resolve(model).then(model.tableDoesNotExistListeners[key])
    }
  },
  createTable: function(tableName, deferPostCreation) {
    var self = this
    var model = this.models[tableName]
    return model.createTable(function() {
      if (!deferPostCreation) {
        self.postTableCreation(model.tableName)
      }
    })
  },
  createTables: function(tableNames) {
    // list of tables to be created synchronously and in order
    var self = this
    return new Promise(function(resolve, reject) {
      var curProm = null
      for (var i=0,l=tableNames.length; i<l; i++) {
        var model = self.models[tableNames[i]]
        var createTable = function() {
          return model.createTable()
        }
        if (!curProm) {
          curProm = createTable()
        } else {
          curProm = curProm.then(function() {
            return createTable()
          }, reject)
        }
      }
      if (curProm) {
        curProm.then(function() {
          for (var i=0,l=tableNames.length; i<l; i++) {
            var model = self.models[tableNames[i]]
            model.createIndexes()
          }
          // don't wait for indexes to be created
          resolve(self)
        }, reject)
      }
    })
  },
  createTableMaybe: function(tableName) {
    var self = this
    // get a promise that will allow us to test if we have to create it and index it
    return this.k.schema.hasTable(tableName).then(
      function(tableExists) {
        if (!tableExists) {
          // unintuitively, this function runs even if the table DOES exist
          // hence the test above
          return self.createTable(tableName)
        }
      })
  },
  createModel: function(tableName, schema, pkDict) {
    //pkDict: see zipcode example
    var fields = {}
    var dateFields = []

    for (var i=0,l=schema.fields.length; i<l; i++) {
      var fdata = schema.fields[i]
      var fieldName = fdata[0]
      var kninkyField = fdata[1]
      fields[fieldName] = kninkyField
      kninkyField.prepWithFieldName(fieldName)
      if (kninkyField.isDate) {
        dateFields.push(fieldName)
      }
    }
    var model = models.dbModel.new(tableName, fields, {}, this)
    model.dateFields = dateFields
    model.pk = (pkDict && pkDict.pk) || 'id'
    model.timestamps = (pkDict && pkDict.timestamps)
    model.noAutoCreation = (process.env.RETHINK_KNEX_NOAUTOCREATE || (pkDict && pkDict.noAutoCreation))

    this.models[tableName] = model

    if (!model.noAutoCreation) {
      this.createTableMaybe(tableName)
    }

    return model
  }
}

module.exports = function(knexConfig) {
  return new dumbThinky(knexConfig);
}
