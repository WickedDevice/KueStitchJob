// jshint esversion: 6
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');

/**************************************************************
                          Utility and DB functions
***************************************************************/

var findDocuments = function(colxn, condition, options = {}) {
  let projection = options.projection || {_id: 0};
  let sort = options.sort;
  let limit = options.limit;
  let skip = options.skip;
  const origProjection = JSON.parse(JSON.stringify(projection));
  projection = Object.assign({}, projection);

  if(projection._id === 0) {
    delete projection._id;
  }

  // Get the documents collection
  return new Promise((resolve, reject) => {
    var url = 'mongodb://127.0.0.1:27017/airqualityegg';
    MongoClient.connect(url, function(err, db) {
      if(err){
        reject(err);
      }
      else {
        // console.log("Connected correctly to server");
        try{
          var collection = db.collection(colxn);
          // Find some documents
          let cursor = collection.find(condition, projection);

          if(sort){
            // console.log('Applying sort', sort);
            cursor = cursor.sort(sort);
          }

          if(skip) {
            cursor = cursor.skip(skip);
          }

          if(limit){
            // console.log('Applying limit', limit);
            cursor = cursor.limit(limit);
          }

          cursor.toArray(function(err, docs) {
            if(err){
              reject(err);
            }
            else{
              resolve(docs.map(d => {
                d.created = moment(d._id.getTimestamp());
                d.created = d.created.format();
                if(origProjection._id === 0) {
                  delete d._id;
                }
                return d;
              }));
            }
            db.close();
          });
        }
        catch(error){
          reject(error);
          db.close();
        }
      }
    });
  });
};


//// BE CAREFUL, upsert: true is default behavior, use options if that is not desired
var updateDocument = function(colxn, condition, update, options = {}){

  let opts = Object.assign({}, {upsert: true}, options);
  let updateOperation = { $set: update }; // simple default use case
  if(opts.updateType === 'complex'){ // this represents intentionality
    delete opts.updateType;
    // if updateType is marked complex defer to caller for a complete
    // update operator specification, rather than a simple $set operation
    updateOperation = update;
    if(updateOperation) {
      if(updateOperation.$set) {
        delete updateOperation.$set._id;
        delete updateOperation.$set.created;
      } else if(updateOperation.$unset) {
        delete updateOperation.$unset._id;
        delete updateOperation.$unset.created;
      }
    }
  } else {
    delete update._id;
    delete update.created;
  }

  // update ONE document in the collection
  return new Promise((resolve, reject) => {
    var url = 'mongodb://127.0.0.1:27017/airqualityegg';
    MongoClient.connect(url, function(err, db) {
      if(err){
        reject(err);
      }
      else {
        // console.log("Connected correctly to server");
        var collection = db.collection(colxn);
        if(opts.updateMany){
          collection.updateMany(condition, updateOperation, opts, function(err, result) {
            if(err){
              reject(err);
            }
            else{
              resolve(result);
            }
            db.close();
          });
        }
        else{
          collection.updateOne(condition, updateOperation, opts, function(err, result) {
            if(err){
              reject(err);
            }
            else{
              resolve(result);
            }
            db.close();
          });
        }
      }
    });
  });
};

var deleteDocument = function(colxn, condition, options = {}){

  let opts = Object.assign({}, {}, options);

  // only allow deletion by specific fields
  let errorMessage = null;
  switch(colxn){
  case 'Users':
    if(Object.keys(condition).indexOf('name') < 0){
      errorMessage = 'Users can only be deleted by \'name\' and none was supplied';
    }
    break;
  case 'Experiments':
    if(Object.keys(condition).indexOf('id') < 0 && Object.keys(condition).indexOf('parent_id') < 0){
      errorMessage = 'Experiments can only be deleted by \'id\' or \'parent_id\' and neither was supplied';
    }
    break;
  case 'Eggs':
    if(Object.keys(condition).indexOf('serial_number') < 0){
      errorMessage = 'Eggs can only be deleted by \'serial_number\' and none was supplied';
    }
    break;
  case 'Blogs':
    if(Object.keys(condition).indexOf('name') < 0){
      errorMessage = 'Blogs can only be deleted by \'name\' and none was supplied';
    }
    break;
    case 'Workflows':
    if(Object.keys(condition).indexOf('id') < 0){
      errorMessage = 'Workflows can only be deleted by \'id\' and none was supplied';
    }
    break;
    case 'EmailTemplates':
    if(Object.keys(condition).indexOf('id') < 0){
      errorMessage = 'EmailTemplates can only be deleted by \'id\' and none was supplied';
    }
    break;
    case 'DeferredEmails':
    if(Object.keys(condition).indexOf('id') < 0){
      errorMessage = 'DeferredEmails can only be deleted by \'id\' and none was supplied';
    }
    break;
  default:
    errorMessage = `Attempted to delete from unsupported collection ${colxn}`;
    break;
  }


  // delete ONE document in the collection
  return new Promise((resolve, reject) => {
    if(!errorMessage){
      var url = 'mongodb://127.0.0.1:27017/airqualityegg';
      MongoClient.connect(url, function(err, db) {
        if(err){
          reject(err);
        }
        else {
          // console.log("Connected correctly to server");
          var collection = db.collection(colxn);
          if(opts.deleteMany) {
            collection.deleteMany(condition, opts, function(err, result) {
              if(err){
                reject(err);
              }
              else{
                resolve(result);
              }
              db.close();
            });
          }
          else {
            collection.deleteOne(condition, opts, function(err, result) {
              if(err){
                reject(err);
              }
              else{
                resolve(result);
              }
              db.close();
            });
          }
        }
      });
    }
    else {
      reject(new Error(errorMessage));
    }
  });
};

function findDistinct(colxn, field) {
  return new Promise((resolve, reject) => {
    var url = 'mongodb://127.0.0.1:27017/airqualityegg';
    MongoClient.connect(url, function(err, db) {
      if(err){
        reject(err);
      }
      else {
        // console.log("Connected correctly to server");
        try{
          var collection = db.collection(colxn);
          // Find some documents
          let cursor = collection.distinct(field, {}, (err, result) => {
            if(err) {
              reject(err);
            } else {
              resolve(result);
            }
          });

        }
        catch(error){
          reject(error);
          db.close();
        }
      }
    });
  });
}

module.exports = {
  findDocuments,
  deleteDocument,
  updateDocument,
  findDistinct
};