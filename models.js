//MORETODO: allow var x = new Model(objData); x.save()

function Document(model, options) {
  this._model = model.__proto__
}

Document.prototype.save = function(callback) {
  //saving an actual document that has possibly been modified
  var res = this._model.save(this, {conflict: 'update'})
  return (callback ? res.then(callback) : res.then())
}


function dbModel(tableName, fields, options, kninky) {
  this.tableName = tableName
  this.kninky = kninky

  this.fields = fields || {}
  this.indexes = {}
  this.pk = 'id'
  this.justCreatedTables = false
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
    if (this.justCreatedTables) {
      this.kninky.k.table(tableName).index(fields)
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
      console.log('SAVE', objData.length, objData[0], options)
      return this.kninky.k.batchInsert(this.tableName, objData, 100)
    } else {
      console.log('SAVE', objData, options)
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
          console.log('SAVE w/defaults', objData)
        }
        return self.kninky.k.insert(objData).into(self.tableName)
          .then(function(ids) {
            //TODO: This needs to be a whole (magical) model thingy WITH fields
            var newData = Object.assign({}, objData)
            if (self.pk == 'id') {
              newData.id = ids[0]
            }
            console.log('SAVE SUCCESS', newData)
            newData.__proto__ = new Document(self, self._options)
            return newData
          })
      }

      if (options && options.conflict == 'update') {
        //STARTHERE: look for val, if so, then send update
        //need to turn the insert into a function which waits on result
        var q = {}
        if (objData[this.pk]) {
          q[this.pk] = objData[this.pk]
        } else {
          Object.assign(q, objData)
        }
        return this.kninky.k.table(this.tableName).where(q)
          .select().then(function(count) {
            if (count.length) {
              return self.kninky.k.table(self.tableName).where(q)
                .update(objData, self.pk).then(function(res) {
                  var newData = Object.assign({}, count[0], objData)
                  console.log('SAVE UPDATE', newData)
                  newData.__proto__ = new Document(self, self._options)
                  return newData
                })
            } else {
              return insertFunc()
            }
          })
      } else {
        return insertFunc()
      }
    }
  },
  update: function(objData) {
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
    return new modelType('date')
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
  stopReference: function() {
    //stops default behavior to link '_id' fields to the referencing table
    this.noReference = true
    return this
  },
  toKnex: function(table, fieldName, knex) {
    var kField = table[this.fieldType](fieldName)
    if (!this.nullable) {
      kField = kField.notNullable()
    }
    if (this.defaultVal) {
      kField = kField.defaultTo(this.defaultVal)
    } else if (this.timestamp && this.timestamp == 'now') {
      kField = kField.defaultTo(knex.fn.now())
    }
    return kField
  }
}

Object.assign(modelType, modelType.prototype)

module.exports = {
  modelType,
  dbModel,
  Document
}
