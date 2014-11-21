'use strict';

var async = require('async');
var fmt   = require('util').format;
var knex  = require('knex');
var pg    = require('pg');

/**
 * Get a livedb client for connecting to a PostgreSQL database.
 *
 * @classdesc A PostgreSQL adapter for livedb
 * @class
 * @param {string} conn a PostgreSQL connection URL
 * @param {string} table a database table name
 */
function LivePg(conn, table) {
  this.conn  = conn;
  this.db    = knex({ client: 'pg', connection: conn });
  this.table = table;
}

/*
 * SNAPSHOT API
 * ============
 */

/**
 * A callback called when getting a document snapshot.
 *
 * @callback LivePg~getSnapshotCallback
 * @param {?Error} err an error
 * @param {?Object} doc document data
 */
/**
 * Get a document snapshot from a given collection.
 *
 * @method
 * @param {string} cName the collection name
 * @param {string} docName the document name
 * @param {LivePg~getSnapshotCallback} cb a callback called with the document
 */
LivePg.prototype.getSnapshot = function getSnapshot(cName, docName, cb) {
  this.db(this.table)
    .where({ collection: cName, name: docName })
    .select('data')
    .limit(1)
    .exec(function onResult(err, rows) {
      if (err) return cb(err, null);
      cb(null, rows.length ? rows[0].data : null);
    });
};

/**
 * A callback called when writing a document snapshot.
 *
 * @callback LivePg~writeSnapshotCallback
 * @param {?Error} err an error
 * @param {?Object} doc document data
 */
/**
 * Write a document snapshot to a given collection.
 *
 * This method uses pg instead of knex because of the complex transaction and
 * locking that's necessary. The lock prevents a race condition between a failed
 * `UPDATE` and the subsequent `INSERT`.
 *
 * @method
 * @param {string} cName the collection name
 * @param {string} docName the document name
 * @param {Object} data the document data
 * @param {LivePg~writeSnapshotCallback} cb a callback called with the document
 */
LivePg.prototype.writeSnapshot = function writeSnapshot(cName, docName, data, cb) {
  var conn  = this.conn;
  var table = this.table;
  var client, done;

  async.waterfall([
    connect,
    begin,
    lock,
    upsert,
    commit
  ], function onDone(err) {
    if (done) done();
    if (err) return cb(err);
    cb(null, data);
  });

  function connect(callback) {
    pg.connect(conn, callback);
  }

  function begin(_client, _done, callback) {
    client = _client;
    done   = _done;
    client.query('BEGIN;', callback);
  }

  function lock(res, callback) {
    var _table = client.escapeIdentifier(table);
    var query  = fmt('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE;', _table);
    client.query(query, callback);
  }

  function upsert(res, callback) {
    var _table   = client.escapeIdentifier(table);

    var update = fmt('UPDATE %s SET data = $1 ' +
      'WHERE collection = $2 AND name = $3', _table);

    var insert = fmt('INSERT INTO %s (collection, name, data) ' +
      'SELECT $2, $3, $1', _table);

    var query = fmt('WITH upsert AS (%s RETURNING *) %s ' +
      'WHERE NOT EXISTS (SELECT * FROM upsert);', update, insert);

    client.query(query, [data, cName, docName], callback);
  }

  function commit(res, callback) {
    client.query('COMMIT;', callback);
  }
};

/**
 * A callback called when getting snapshots in bulk.
 *
 * @callback LivePg~bulkGetSnapshotCallback
 * @param {?Error} err an error
 * @param {?Object} results the results
 */
/**
 * Get specific documents from multiple collections.
 *
 * @method
 * @param {Object} requests the requests documents
 * @param {LivePg~bulkGetSnapshotCallback} cb a callback called with the results
 */
LivePg.prototype.bulkGetSnapshot = function bulkGetSnapshot(requests, cb) {
  var collections = Object.keys(requests);

  async.parallel(collections.map(function eachCollection(cName) {
    var docNames = requests[cName];

    return function getFromCollection(callback) {
      this.fromCollection(cName, docNames, callback);
    }.bind(this);
  }.bind(this)), function onDone(err, results) {
    if (err) return cb(err);
    cb(null, results.reduce(function eachResult(obj, result, i) {
      obj[collections[i]] = result;
      return obj;
    }, {}));
  });
};

/**
 * A callback called when getting snapshots for a given collection.
 *
 * @callback LivePg~fromCollectionCallback
 * @private
 * @param {?Error} err an error
 * @param {?Object} results an set of results for the found documents
 */
/**
 * Get specific documents from a given collection.
 *
 * @method
 * @private
 * @param {string} cName the collection name
 * @param {Array} docNames the document names
 * @param {LivePg~fromCollectionCallback} cb a callback called with results
 */
LivePg.prototype.fromCollection = function fromCollection(cName, docNames, cb) {
  this.db(this.table)
    .whereIn('name', docNames)
    .andWhere({ collection: cName })
    .select('name', 'data')
    .exec(function onResult(err, results) {
      if (err) return cb(err, null);
      cb(null, results.reduce(function eachResult(obj, result) {
        obj[result.name] = result.data;
        return obj;
      }, {}));
    });
};

/*
 * OPERATION API
 * =============
 */

/**
 * A callback called when writing an operation
 *
 * @callback LivePg~writeOpCallback
 * @param {?Error} err an error
 * @param {?Object} op the operation data
 */
/**
 * Write an operation.
 *
 * @method
 * @param {string} cName a collection name
 * @param {string} docName a document name
 * @param {Object} opData the operation data
 * @param {LivePg~writeOpCallback} cb a callback called with the op data
 */
LivePg.prototype.writeOp = function writeOp(cName, docName, opData, cb) {
  this.db(this.table)
    .insert({
      collection_name: cName,
      document_name  : docName,
      version        : opData.v,
      data           : opData
    })
    .returning('data')
    .exec(function onResult(err, rows) {
      if (err) return cb(err, null);
      cb(null, rows.length ? rows[0] : null);
    });
};

/**
 * A callback called with the next version of the document
 *
 * @callback LivePg~getVersionCallback
 * @param {?Error} err an error
 * @param {?number} version the next document version
 */
/**
 * Get the next document version.
 *
 * @method
 * @param {string} cName a collection name
 * @param {string} docName a document name
 * @param {LivePg~getVersionCallback} cb a callback called with the next version
 */
LivePg.prototype.getVersion = function getVersion(cName, docName, cb) {
  this.db(this.table)
    .where({ collection_name: cName, document_name: docName })
    .select('version')
    .orderBy('version', 'desc')
    .limit(1)
    .exec(function onResult(err, rows) {
      if (err) return cb(err);
      cb(null, rows.length ? parseInt(rows[0].version, 10) + 1 : 0);
    });
};

/**
 * A callback called with ops
 *
 * @callback LivePg~getOpsCallback
 * @param {?Error} err an error
 * @param {?Array(Object)} ops the requested ops
 */
/**
 * Get operations between `start` and `end`, noninclusively.
 *
 * @method
 * @param {string} cName a collection name
 * @param {string} docName a document name
 * @param {number} start the start version
 * @param {end} end the end version
 * @param {LivePg~getOpsCallback} cb a callback called with the ops
 */
LivePg.prototype.getOps = function getOps(cName, docName, start, end, cb) {
  var query = this.db(this.table).where('version', '>=', start);

  if (typeof end === 'number') {
    query.andWhere('version', '<', end);
  }

  query.select('data')
    .orderBy('version', 'asc')
    .exec(function onResult(err, rows) {
      if (err) return cb(err);
      cb(null, rows.map(function eachRow(row) {
        return row.data;
      }));
    });
};

module.exports = LivePg;
