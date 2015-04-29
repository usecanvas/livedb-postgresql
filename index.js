'use strict';

const async = require('async');
const knex  = require('knex');
const pg    = require('pg');

pg.on('end', () => LivePg.willClose = true);

class LivePg {
  constructor(conn, table) {
    this.conn  = conn;
    this.db    = knex({ client: 'pg', connection: conn });
    this.table = table;
  }

  getSnapshot(cName, docName, cb) {
    this.db(this.table)
      .where({ collection: cName, name: docName })
      .select('data')
      .limit(1)
      .exec((err, rows) => {
        if (err) return cb(err, null);
        cb(null, rows.length ? rows[0].data : null);
      });
  }

  writeSnapshot(cName, docName, data, cb) {
    const { conn, table } = this;
    let client, done;

    async.waterfall([
      connect,
      begin,
      lock,
      upsert,
      commit
    ], err => {
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
      const _table = client.escapeIdentifier(table);
      const query  = `LOCK TABLE ${_table} IN SHARE ROW EXCLUSIVE MODE;`;
      client.query(query, callback);
    }

    function upsert(res, callback) {
      const _table = client.escapeIdentifier(table);

      const update = `UPDATE ${_table}
        SET DATA = $1 WHERE collection = $2 AND name = $3`;

      const insert = `INSERT INTO ${_table} (collection, name, data)
        SELECT $2, $3, $1`;

      const query = `WITH upsert AS (${update} RETURNING *) ${insert}
        WHERE NOT EXISTS (SELECT * FROM upsert);`;

      client.query(query, [data, cName, docName], callback);
    }

    function commit(res, callback) {
      client.query('COMMIT;', callback);
    }
  }

  bulkGetSnapshot(requests, cb) {
    const colls = Object.keys(requests);
    const query = this.db(this.table).select('collection', 'name', 'data');

    colls.forEach(cName => {
      query
        .orWhereIn('name', requests[cName])
        .andWhere({ collection: cName });
    });

    query.exec((err, results) => {
      if (err) return cb(err, null);

      results = results.reduce((obj, result) => {
        obj[result.collection] = obj[result.collection] || {};
        obj[result.collection][result.name] = result.data;
        return obj;
      }, {});

      // Add colls with no documents found back to the results
      for (let i = 0, len = colls.length; i < len; i++) {
        if (!results[colls[i]]) results[colls[i]] = {};
      }

      cb(null, results);
    });
  }

  writeOp(cName, docName, opData, cb) {
    this.db(this.table)
      .insert({
        collection_name: cName,
        document_name  : docName,
        version        : opData.v,
        data           : opData
      })
      .returning('data')
      .exec(err => {
        if (err && err.code !== '23505')
          return cb(err, null);
        cb(null, opData);
      });
  }

  getVersion(cName, docName, cb) {
    this.db(this.table)
      .where({ collection_name: cName, document_name: docName })
      .select('version')
      .orderBy('version', 'desc')
      .limit(1)
      .exec((err, rows) => {
        if (err) return cb(err, null);
        cb(null, rows.length ? parseInt(rows[0].version, 10) + 1 : 0);
      });
  }

  getOps(cName, docName, start, end, cb) {
    const query = this.db(this.table)
      .where('version', '>=', start)
      .andWhere({ collection_name: cName, document_name: docName });

    if (typeof end === 'number') {
      query.andWhere('version', '<', end);
    }

    query.select('data')
      .orderBy('version', 'asc')
      .exec((err, rows) => {
        if (err) return cb(err, null);
        cb(null, rows.map(row => row.data));
      });
  }

  close(cb) {
    LivePg.close(cb);
  }

  static close(cb) {
    if (this.willClose) {
      cb();
    } else {
      pg.once('end', cb);
      pg.end();
    }
  }
}

module.exports = LivePg;
