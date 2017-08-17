var rethinkQuery = require('./query')
var models = require('./models')

//import rethinkQuery from './query'
//import {modelType} from './models'

function dumbThinky(knexConfig) {
  // for 'direct' access to knex
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
  createModel: function(tableName, schema, pkDict) {
    //pkDict: see zipcode example
    var self = this
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
    this.models[tableName] = model
    model.pk = (pkDict && pkDict.pk) || 'id'

    // get a promise that will allow us to test if we have to create it and index it
    model.doesTableExist = this.k.schema.hasTable(tableName)

    model.doesTableExist.then(
      function(tableExists) {
        if (!tableExists) {
          // unintuitively, this function runs even if the table DOES exist
          // hence the test above
          self.k.schema.createTableIfNotExists(tableName, function (table) {
            if ('id' in fields && fields['id'].fieldType == 'string') {
              table.uuid('id') // FUTURE: the uuid would need to come client-side for this to work
            } else if (!pkDict || !pkDict.pk) {
              table.increments(); //default 'id' field
            }
            if (pkDict && pkDict.timestamps) {
              table.timestamps();
            }
            for (var fieldName in fields) {
              if (fieldName === 'id') {
                continue // addressed above
              }
              var kninkyField = fields[fieldName]
              var kField = kninkyField.toKnex(table, fieldName, self.k)
              // is primary key?
              if (pkDict && pkDict.pk && pkDict.pk == fieldName) {
                kField = kField.primary()
              }
            }
            model.justCreatedTable = true
            return model
          }).catch(function(err) {
            console.error('failed to create ', tableName, err)
            return null
          });
        }
      })
    return model
  }
}

module.exports = function(knexConfig) {
  return new dumbThinky(knexConfig);
}
