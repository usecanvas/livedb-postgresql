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

module.exports = LivePg;
