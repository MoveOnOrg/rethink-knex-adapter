var rethinkQuery = require('./query')
var models = require('./models')
//import rethinkQuery from './query'
//import {modelType} from './models'

function dumbThinky(knexConfig) {
    // for 'direct' access to knex
    this.k = require('knex')(knexConfig)

    // implementation of (some of) thinky/rethinkdb driver interfaces
    this.r = rethinkQuery(this)
    this.type = models.modelType

    // list of models 'created' by createModel()
    this.models = {}
}

dumbThinky.prototype = {
  createModel: function(tableName, schema, pkDict) {
    //pkDict: see zipcode example
    var fields = {};
    for (var i=0,l=schema.fields.length; i<l; i++) {
      var fdata = schema.fields[i]
      var fieldName = fdata[0]
      var kninkyField = fdata[1]
      fields[fieldName] = kninkyField
    }
    var model = new models.dbModel(this, tableName, fields)

    this.k.schema.createTableIfNotExists(tableName, function (table) {
      if ('id' in fields && fields['id'].fieldType == 'string') {
        table.uuid('id')
      } else {
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
        var kField = kninkyField.toKnex(table, fieldName)
        // is a foreign key?
        if (fieldName.endsWith('_id') && !kninkyField.noReference) {
          var refTable = fieldName.split('_id')[0]
          kField = kField.references('id').inTable(refTable)
        }
        // is primary key?
        if (pkDict && pkDict.pk && pkDict.pk == fieldName) {
          kField = kField.primary()
        }
      }
      model.justCreatedTables = true
    })
    return model
  }
}

module.exports = dumbThinky
