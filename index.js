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
    var fields = model.fields
    return new Promise(function(resolve, reject) {
      var pp = self.k.schema.createTableIfNotExists(tableName, function (table) {
        if ('id' in fields && fields['id'].fieldType == 'string') {
          table.uuid('id') // FUTURE: the uuid would need to come client-side for this to work
        } else if (model.pk === 'id') {
          table.increments(); //default 'id' field
        }
        if (model.timestamps) {
          table.timestamps();
        }
        for (var fieldName in fields) {
          if (fieldName === 'id') {
            continue // addressed above
          }
          var kninkyField = fields[fieldName]
          var kField = kninkyField.toKnex(table, fieldName, self.k)
          // is primary key?
          if (model.pk == fieldName) {
            kField = kField.primary()
          }
        }
        model.tableDidNotExist = true
        if (!deferPostCreation) {
          self.postTableCreation(model.tableName)
        }
        return tableName
      }).catch(function(err) {
        console.error('failed to create ', tableName, err)
      });
      return pp.then(resolve, reject)
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
    this.models[tableName] = model

    if (!process.env.RETHINK_KNEX_NOAUTOCREATE && (!pkDict || !pkDict.noAutoCreation)) {
      this.createTableMaybe(tableName)
    }

    return model
  }
}

module.exports = function(knexConfig) {
  return new dumbThinky(knexConfig);
}
