'use strict';

var async    = require('async');
var fmt      = require('util').format;
var inherits = require('util').inherits;
var knex     = require('knex');
var pg       = require('pg');

pg.on('end', function onPgEnd() {
  LivePg.willClose = true;
});

/**
 * Get a livedb client for connecting to a PostgreSQL database.
 *
 * @classdesc A PostgreSQL adapter for livedb
 * @class
 * @param {object} opts An object of options
 * @param {string} opts.conn A PostgreSQL connection URL
 * @param {string} opts.db An optional existing Knex database client
 * @param {string} opts.table A database table name
 */
function LivePg(opts) {
  this.conn  = required(opts, 'conn');
  this.table = required(opts, 'table');
  this.db    = opts.db || knex({ client: 'pg', connection: this.conn });
}

/**
 * Get a livedb client for snapshots.
 *
 * @classdesc A PostgreSQL adapter for livedb snapshots
 * @class
 * @extends LivePg
 * @param {object} opts An object of options
 * @param {string} opts.collectionColumn The name of the column in the
 *   snapshot table with the collection
 * @param {string} opts.nameColumn The name of the column in the
 *   snapshot table with the document
 * @param {string} opts.dataColumn The name of the column in the
 *   snapshot table with the snapshot data
 * @param {Function?} opts.snapshotWriteLock A function called with a DB client
 *   and a callback that should lock the snapshots table. Call the callback with
 *   an error, otherwise with `(null, null)`.
 */
LivePg.Snapshots = function LivePgSnapshots(opts) {
  LivePg.call(this, opts);

  this.collectionColumn  = defaults(opts, 'collectionColumn', 'collection');
  this.dataColumn        = defaults(opts, 'dataColumn', 'data');
  this.nameColumn        = defaults(opts, 'nameColumn', 'name');
  this.snapshotWriteLock = opts.snapshotWriteLock;
};

inherits(LivePg.Snapshots, LivePg);

/*
 * SNAPSHOT API
 * ============
 */

/**
 * A callback called when getting a document snapshot.
 *
 * @callback LivePg.Snapshots~getSnapshotCallback
 * @param {?Error} err an error
 * @param {?Object} doc document data
 */
/**
 * Get a document snapshot from a given collection.
 *
 * @method
 * @param {string} cName the collection name
 * @param {string} docName the document name
 * @param {LivePg.Snapshots~getSnapshotCallback} cb a callback called with the
 *   document
 */
LivePg.Snapshots.prototype.getSnapshot = function getSnapshot(cName, docName, cb) {
  var where = {};
  where[this.collectionColumn] = cName;
  where[this.nameColumn]       = docName;

  var dataColumn = this.dataColumn;

  this.db(this.table)
    .where(where)
    .select(dataColumn)
    .limit(1)
    .asCallback(function onResult(err, rows) {
      if (err) return cb(err, null);
      cb(null, rows.length ? rows[0][dataColumn] : null);
    });
};

/**
 * A callback called when writing a document snapshot.
 *
 * @callback LivePg.Snapshots~writeSnapshotCallback
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
 * @param {LivePg.Snapshots~writeSnapshotCallback} cb a callback called with the
 *   document
 */
LivePg.Snapshots.prototype.writeSnapshot = function writeSnapshot(cName, docName, data, cb) {
  var conn  = this.conn;
  var table = this.table;
  var client, done;

  var collectionColumn  = this.collectionColumn;
  var dataColumn        = this.dataColumn;
  var nameColumn        = this.nameColumn;
  var snapshotWriteLock = this.snapshotWriteLock;

  async.waterfall([
    connect,
    begin,
    lock,
    upsert,
    commit
  ], function onDone(err) {
    if (done) done();
    if (err) return cb(err, null);
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
    if (snapshotWriteLock) {
      snapshotWriteLock(client, callback);
      return;
    }

    var _table = client.escapeIdentifier(table);
    var query  = fmt('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE;', _table);
    client.query(query, callback);
  }

  function upsert(res, callback) {
    var _table = client.escapeIdentifier(table);

    var update = fmt('UPDATE %s SET %s = $1 WHERE %s = $2 AND %s = $3',
      _table, dataColumn, collectionColumn, nameColumn);

    var insert = fmt('INSERT INTO %s (%s, %s, %s) SELECT $2, $3, $1',
      _table, collectionColumn, nameColumn, dataColumn);

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
 * @callback LivePg.Snapshots~bulkGetSnapshotCallback
 * @param {?Error} err an error
 * @param {?Object} results the results
 */
/**
 * Get specific documents from multiple collections.
 *
 * @method
 * @param {Object} requests the requests documents
 * @param {LivePg.Snapshots~bulkGetSnapshotCallback} cb a callback called with
 *   the results
 */
LivePg.Snapshots.prototype.bulkGetSnapshot = function bulkGetSnapshot(requests, cb) {
  var collections              = Object.keys(requests);
  var collectionColumn = this.collectionColumn;
  var nameColumn       = this.nameColumn;
  var dataColumn       = this.dataColumn;

  var query = this.db(this.table).select(
    collectionColumn, nameColumn, dataColumn
  );

  collections.forEach(function eachCName(cName) {
    var andWhere = {};
    andWhere[collectionColumn] = cName;

    query
      .orWhereIn(nameColumn, requests[cName])
      .andWhere(andWhere);
  });

  query.asCallback(function onDone(err, results) {
    if (err) return cb(err, null);

    results = results.reduce(function eachResult(obj, result) {
      obj[result[collectionColumn]] =
        obj[result[collectionColumn]] || {};
      obj[result[collectionColumn]][result[nameColumn]] =
        result[dataColumn];
      return obj;
    }, {});

    // Add collections with no documents found back to the results
    for (var i = 0, len = collections.length; i < len; i++) {
      if (!results[collections[i]]) results[collections[i]] = {};
    }

    cb(null, results);
  });
};

/*
 * OPERATION API
 * =============
 */

/**
 * Get a livedb client for operations.
 *
 * @classdesc A PostgreSQL adapter for livedb operations
 * @class
 * @extends LivePg
 * @param {object} opts An object of options
 * @param {string} opts.collectionColumn The name of the column in the op
 *   table with the collection
 * @param {string} opts.documentColumn The name of the column in the op table
 *   with the document
 * @param {string} opts.dataColumn The name of the column in the op table with
 *   the op data
 * @param {string} opts.versionColumn The name of the column in the op table
 *   with the op version
 */
LivePg.OpLog = function LivePgOpLog(opts) {
  LivePg.call(this, opts);

  this.collectionColumn = defaults(opts, 'collectionColumn', 'collection_name');
  this.documentColumn   = defaults(opts, 'documentColumn', 'document_name');
  this.dataColumn       = defaults(opts, 'dataColumn', 'data');
  this.versionColumn    = defaults(opts, 'versionColumn', 'version');
};

inherits(LivePg.OpLog, LivePg);

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
 * @param {Object} data the operation data
 * @param {LivePg~writeOpCallback} cb a callback called with the op data
 */
LivePg.OpLog.prototype.writeOp = function writeOp(cName, docName, data, cb) {
  var insert = {};
  insert[this.versionColumn]    = data.v;
  insert[this.dataColumn]       = data;
  insert[this.collectionColumn] = cName;
  insert[this.documentColumn]   = docName;

  this.db(this.table)
    .insert(insert)
    .returning(this.dataColumn)
    .asCallback(function onResult(err) {
      if (err && err.code !== '23505')
        return cb(err, null);
      cb(null, data);
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
LivePg.OpLog.prototype.getVersion = function getVersion(cName, docName, cb) {
  var where = {};
  where[this.collectionColumn] = cName;
  where[this.documentColumn]   = docName;

  var versionColumn = this.versionColumn;

  this.db(this.table)
    .where(where)
    .select(versionColumn)
    .orderBy(versionColumn, 'desc')
    .limit(1)
    .asCallback(function onResult(err, rows) {
      if (err) return cb(err, null);
      cb(null, rows.length ? parseInt(rows[0][versionColumn], 10) + 1 : 0);
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
 * @param {?end} end the end version
 * @param {LivePg~getOpsCallback} cb a callback called with the ops
 */
LivePg.OpLog.prototype.getOps = function getOps(cName, docName, start, end, cb) {
  var andWhere = {};
  andWhere[this.collectionColumn] = cName;
  andWhere[this.documentColumn]   = docName;

  var dataColumn    = this.dataColumn;
  var versionColumn = this.versionColumn;

  var query = this.db(this.table)
    .where(versionColumn, '>=', start)
    .andWhere(andWhere);

  if (typeof end === 'number') {
    query.andWhere(versionColumn, '<', end);
  }

  query.select(dataColumn)
    .orderBy(versionColumn, 'asc')
    .asCallback(function onResult(err, rows) {
      if (err) return cb(err, null);
      cb(null, rows.map(function eachRow(row) {
        return row[dataColumn];
      }));
    });
};

/**
 * Close the connection to the database.
 *
 * @method
 * @param {LivePg~closeCallback} cb a callback called when the connection closes
 */
LivePg.prototype.close = function close(cb) {
  LivePg.close(cb);
};

/**
 * A callback called when the database connection has closed
 *
 * @callback LivePg~closeCallback
 * @static
 */
/**
 * Close the connection to the database.
 *
 * @method
 * @static
 * @param {LivePg~closeCallback} cb a callback called when the connection closes
 */
LivePg.close = function close(cb) {
  if (this.willClose) {
    cb();
  } else {
    pg.once('end', cb);
    pg.end();
  }
};

function defaults(opts, key, defaultValue) {
  return opts[key] === undefined ? defaultValue : opts[key];
}

function required(opts, key) {
  if (!opts[key]) {
    throw new Error('Key "' + key + '" is required in LivePg constructor');
  }

  return opts[key];
}

module.exports = LivePg;
