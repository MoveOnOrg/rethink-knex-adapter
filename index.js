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
    for (var i=0,l=schema.fields.length; i<l; i++) {
      var fdata = schema.fields[i]
      var fieldName = fdata[0]
      var kninkyField = fdata[1]
      fields[fieldName] = kninkyField
    }
    var model = models.dbModel.new(tableName, fields, {}, this)
    this.models[tableName] = model

    this.k.schema.createTableIfNotExists(tableName, function (table) {
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
        if (kninkyField.isDate) {
          model.dateFields.push(fieldName)
        }
        // is a foreign key?
        if ((fieldName.endsWith('_id') || kninkyField.foreignReference) && !kninkyField.noReference) {
          var refTable = kninkyField.foreignReference || fieldName.split('_id')[0]
          kField = kField.references('id').inTable(refTable)
          if (!kninkyField.nullable && kninkyField.defaultVal == '') {
            //stupid rethink pattern of foreign keys being allowNull(false).default('')
            kField = kField.nullable()
          }
        }
        // is primary key?
        if (pkDict && pkDict.pk && pkDict.pk == fieldName) {
          kField = kField.primary()
          model.pk = fieldName
        }
      }
      model.justCreatedTables = true
    }).catch(function(err) {
      console.error('failed to create ', tableName, err)
    })
    return model
  }
}

module.exports = function(knexConfig) {
  return new dumbThinky(knexConfig);
}
