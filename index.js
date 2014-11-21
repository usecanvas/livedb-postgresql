'use strict';

var knex = require('knex');

/**
 * Get a livedb client for connecting to a PostgreSQL database.
 *
 * @classdesc A PostgreSQL adapter for livedb
 * @class
 * @param {string} conn a PostgreSQL connection URL
 * @param {string} table a database table name
 */
function LivePg(conn, table) {
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
 * @method
 * @param {string} cName the collection name
 * @param {string} docName the document name
 * @param {Object} data the document data
 * @param {LivePg~writeSnapshotCallback} cb a callback called with the document
 */
LivePg.prototype.writeSnapshot = function writeSnapshot(cName, docName, data, cb) {
  this.db(this.table)
    .insert({ collection: cName, name: docName, data: data })
    .returning('data')
    .exec(function onResult(err, rows) {
      if (err) return cb(err, null);
      cb(null, rows[0]);
    });
};

module.exports = LivePg;
