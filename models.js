var Promise = require('bluebird');


function log() {
  if (process.env.RETHINK_KNEX_DEBUG) {
    console.log.apply(null, arguments)
  }
}

function Document(model, options, exists) {
  this._model = model.__proto__
  this._exists = exists || false
}

Document.prototype.save = function(callback) {
  //saving an actual document that has possibly been modified
  var self = this
  var options = {}
  if (this._exists || this[this._model.pk]) {
    options['conflict'] = 'update'
  }
  log('RUNNING document.save()')
  var res = this._model.save(this, options)
  var updateSavedObj = function(newData) {
    if (newData && newData.id) {
      self.id = newData.id
    }
    return newData
  }
  return (callback ? res.then(updateSavedObj).then(callback) : res.then(updateSavedObj))
}

function dbModel(tableName, fields, options, kninky) {
  this.tableName = tableName
  this.kninky = kninky

  this.fields = fields || {}
  this.dateFields = []
  this.indexes = {}
  this.pk = 'id'
  this.changeListeners = []
  this.tableDidNotExist = false
  this.tableDoesNotExistListeners = {}
  // for supporting introspection
  this._schema = {
    _schema: this.fields,
    _model: {
      _name: tableName
    }
  }
  // probably useless
  this._options = options || {}
}

dbModel.new = function(name, fields, options, kninky) {
  var proto = new dbModel(name, fields, options, kninky)
  var model = function model(doc, options) {
    doc.__proto__ = new Document(model, options)
    return doc
  }
  model.__proto__ = proto
  return model
}

dbModel.prototype = {
  _ifTableDoesNotExist: function(key, func) {
    if (this.tableDidNotExist
        && !(key in this.tableDoesNotExistListeners)) {
      // case where we must have already run and completed the test
      // but we also check to make sure the same key-func wasn't already run
      Promise.resolve(this).then(func)
    } else {
      // either before the test was run OR after it was run and we learned it existed
      // in the second case, adding it does no harm
      this.tableDoesNotExistListeners[key] = func
    }
  },
  ensureIndex: function(indexName, indexFunc) {
    var fields = [];
    if (indexFunc) {
      indexFunc(function(fname) {
        fields.push(fname)
      })
    } else {
      fields.push(indexName)
    }
    this.indexes[indexName] = fields;
    var createIndex = function(self) {
      self.kninky.k.schema.alterTable(self.tableName, function(table) {
        table.index(fields)
      }).then(function() {
        log('index "' + indexName + '"created for ' + self.tableName)
      })
    }
    if (process.env.RETHINK_KNEX_FORCE_INDEXCREATION) {
      createIndex(this)
    } else {
      this._ifTableDoesNotExist(indexName, createIndex)
    }
  },
  filter: function(data) {
    return this.kninky.r.table(this.tableName).filter(data)
  },
  get: function(pkVal) {
    return this.kninky.r.table(this.tableName).get(pkVal)
  },
  getAll: function() {
    var q = this.kninky.r.table(this.tableName)
    return q.getAll.apply(q, arguments)
  },
  save: function(objData, options) {
    // MORETODO: returns a promise at the moment
    // mostly NOT DONE. options only has {conflict: 'update'} possibility
    var self = this
    if (Array.isArray(objData)) {
      objData = objData.map(this._prepSaveFields.bind(this))
      log('SAVE BATCH', objData.length, objData[0], options)
      return this.kninky.k.batchInsert(this.tableName, objData, 100).then(
        function(d) {
          //batchInsert returns an array of create counts (or ids?)
          // like [100, 200, 300, ....]
          //MORETODO: not sure we need to make this return right thing
          return d
        },
        function(err) {
          console.error('BATCH INSERT ERROR', err)
        })
    } else {
      log('SAVE', objData, options)
      this._prepSaveFields(objData)
      this._prepSaveFields(this)
      var insertFunc = function() {
        // only set defaults on insert -- not on update
        if (self.kninky.defaultsUnsupported) {
          for (var a in self.fields) {
            var f = self.fields[a]
            if (typeof objData[a] == 'undefined'
                && typeof f.defaultVal != 'undefined') {
              objData[a] = f.defaultVal
            }
          }
          log('SAVE w/defaults', objData)
        }
        return self.kninky.k.insert(objData, [self.pk]).into(self.tableName)
          .then(function(ids) {
            log('POST SAVE ARGS', ids)
            var newData = Object.assign({}, objData)
            if (ids.length && self.pk == 'id') {
              // postgres return [{id:123}] whereas everyone
              // else returns [123] (wtf?!)
              newData.id = ids[0][self.pk] || ids[0]
            }
            log('SAVE SUCCESS', newData)
            newData.__proto__ = new Document(self, self._options, true)
            self._callChangeListeners(newData, null)
            return newData
          })
      }

      if (options && options.conflict == 'update') {
        var q = {}
        if (objData[this.pk]) {
          q[this.pk] = objData[this.pk]
        } else {
          Object.assign(q, objData)
        }
        return this.kninky.k.table(this.tableName).where(q)
          .select().then(function(serverData) {
            if (serverData.length) {
              return self.update(objData, serverData[0], q)
            } else {
              return insertFunc()
            }
          })
      } else {
        return insertFunc()
      }
    }
  },
  update: function(objData, serverData, q) {
    var self = this
    this._prepSaveFields(objData)
    this._prepSaveFields(this)
    if (!q) {
      var pkVal = objData[this.pk]
      if (pkVal) {
        q = {}
        q[this.pk] = objData[this.pk]
      } else {
        throw new Error("cannot update unsaved value")
      }
    }
    return this.kninky.k.table(this.tableName).where(q)
      .update(objData, this.pk).then(function(res) {
        var newData = Object.assign({}, serverData, objData)
        log('SAVE UPDATE', newData)
        newData.__proto__ = new Document(self, self._options, true)
        newData.replaced = 1
        self._callChangeListeners(newData, serverData || self)
        return newData
      })
  },
  _prepSaveFields: function(objData) {
    // also used to prep query dictionaries in query.js
    if (objData.replaced && !this.fields.replaced) {
      delete objData.replaced
      delete objData.__proto__.replaced
    }
    if (objData._exists && !this.fields._exists) {
      delete objData._exists
      delete objData.__proto__._exists
    }
    for (var f in objData) {
      if (this.fields[f] && this.fields[f].fieldType == 'integer') {
        if (objData[f]) {
          objData[f] = parseInt(objData[f])
        } else if (objData[f] === '') {
          objData[f] = null
        }
      }
    }
    return objData
  },
  _updateDateFields: function(objData) {
    // converts date field numbers/strings into date objects
    // which is how (some) databases (cough *sqlite*) return the data
    for (var i=0,l=this.dateFields.length; i<l; i++) {
      var f = this.dateFields[i]
      if (objData[f] && !(objData[f] instanceof Date)) {
        objData[f] = new Date(objData[f])
      }
    }
    return objData
  },
  _callChangeListeners: function(newObjData, oldObjData) {
    for (var i=0,l=this.changeListeners.length; i<l; i++) {
      Promise.resolve({new_val: newObjData,
                       old_val: oldObjData})
        .then(this.changeListeners[i])
    }
  }
}

function modelType(fieldType) {
  //fieldType needs to be a knex type:
  // http://knexjs.org/#Schema-Building
  this.fieldType = fieldType
  this.isRequired = false
  this.nullable = true
}

modelType.prototype = {
  object: function() {
    //just supporting schema -- no other 'object' types
    // supported for migrations -- change your schema first!
    return {
      schema: function(types) {
        // this needs to return the thing that is parsed/consumed by createModel
        // e.g. thinky.createModel('user', type.object().schema({...}).allowExtra(false))
        var schemaObj = {
          fields: [],
          allowExtra: function(extraAllowed) {
            //extraAllowed is boolean
            schemaObj.extraAllowed = extraAllowed
            return schemaObj
          }
        }
        for (var f in types) {
          schemaObj.fields.push([f, types[f]]);
        } 
        return schemaObj;
      }
    }
  },
  //These need to return a NEW modelType (or other class) that follows builder pattern
  string: function() {
    return new modelType('text')
  },
  boolean: function() {
    return new modelType('boolean')
  },
  number: function() {
    return new modelType('float')
  },
  integer: function() {
    return new modelType('integer')
  },
  date: function() {
    // rethink really means a datetime stamp
    var dt = new modelType('dateTime')
    dt.isDate = true
    return dt
  },
  point: function() {
    //GEO :-(
    // someday use this: https://www.npmjs.com/package/knex-postgis
    throw new Exception("point not supported in knex")
  },

  //Decorators:
  required: function() {
    //not knex-useful, but let's store it
    this.isRequired = true
    return this
  },
  default: function(defaultVal) {
    this.defaultVal = defaultVal
    if (defaultVal && defaultVal == 'r.now()') {
      delete this.defaultVal
      this.timestamp = 'now'
      this.fieldType = 'timestamp'
    }
    return this
  },
  allowNull: function(nullable) {
    this.nullable = nullable
    return this
  },
  min: function(minVal) {
    this.minVal = minVal
    return this
  },
  max: function(maxVal) {
    this.maxVal = maxVal
    return this
  },
  enum: function(options) {
    this.options = options
    return this
  },
  //conversion
  foreign: function(tableName) {
    this.foreignReference = tableName
    return this
  },
  stopReference: function() {
    //stops default behavior to link '_id' fields to the referencing table
    this.noReference = true
    return this
  },
  prepWithFieldName: function(fieldName) {
    this.fieldName = fieldName
    // additional work to prep foreignReferences
    if ((fieldName.endsWith('_id') || this.foreignReference) && !this.noReference) {
      // need to morph reference fields into integers
      if (!this.foreignReference) {
        this.foreignReference = fieldName.split('_id')[0]
      }
      this.fieldType = 'integer'
      if (this.nullable || this.defaultVal == '') {
        //stupid rethink pattern of foreign keys being allowNull(false).default('')
        this.nullable = true
        this.defaultVal = null
      }
    }
  },
  toKnex: function(table, fieldName, knex) {
    var kField;
    this.prepWithFieldName(fieldName)
    if (table) {
      if (this.foreignReference && !this.noReference) {
        kField = table[this.fieldType](fieldName).references('id').inTable(this.foreignReference)
      } else {
        kField = table[this.fieldType](fieldName)
        if (this.timestamp && this.timestamp == 'now') {
          kField = kField.defaultTo(knex.fn.now())
        }
      }

      if (this.hasOwnProperty('defaultVal')) {
        kField = kField.defaultTo(this.defaultVal)
      }

      if (this.nullable) {
        kField = kField.nullable()
      } else {
        kField = kField.notNullable()
      }

      return kField
    }
  }
}

Object.assign(modelType, modelType.prototype)

module.exports = {
  modelType,
  dbModel,
  Document
}
