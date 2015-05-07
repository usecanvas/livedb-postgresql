'use strict';

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

var async = require('async');
var knex = require('knex');
var pg = require('pg');

pg.on('end', function () {
  return LivePg.willClose = true;
});

var LivePg = (function () {
  function LivePg(conn, table) {
    _classCallCheck(this, LivePg);

    this.conn = conn;
    this.db = knex({ client: 'pg', connection: conn });
    this.table = table;
  }

  _createClass(LivePg, [{
    key: 'getSnapshot',
    value: function getSnapshot(cName, docName, cb) {
      this.db(this.table).where({ collection: cName, name: docName }).select('data').limit(1).exec(function (err, rows) {
        if (err) return cb(err, null);
        cb(null, rows.length ? rows[0].data : null);
      });
    }
  }, {
    key: 'writeSnapshot',
    value: function writeSnapshot(cName, docName, data, cb) {
      var conn = this.conn;
      var table = this.table;

      var client = undefined,
          done = undefined;

      async.waterfall([connect, begin, lock, upsert, commit], function (err) {
        if (done) done();
        if (err) return cb(err, null);
        cb(null, data);
      });

      function connect(callback) {
        pg.connect(conn, callback);
      }

      function begin(_client, _done, callback) {
        client = _client;
        done = _done;
        client.query('BEGIN;', callback);
      }

      function lock(res, callback) {
        var _table = client.escapeIdentifier(table);
        var query = 'LOCK TABLE ' + _table + ' IN SHARE ROW EXCLUSIVE MODE;';
        client.query(query, callback);
      }

      function upsert(res, callback) {
        var _table = client.escapeIdentifier(table);

        var update = 'UPDATE ' + _table + '\n        SET DATA = $1 WHERE collection = $2 AND name = $3';

        var insert = 'INSERT INTO ' + _table + ' (collection, name, data)\n        SELECT $2, $3, $1';

        var query = 'WITH upsert AS (' + update + ' RETURNING *) ' + insert + '\n        WHERE NOT EXISTS (SELECT * FROM upsert);';

        client.query(query, [data, cName, docName], callback);
      }

      function commit(res, callback) {
        client.query('COMMIT;', callback);
      }
    }
  }, {
    key: 'bulkGetSnapshot',
    value: function bulkGetSnapshot(requests, cb) {
      var colls = Object.keys(requests);
      var query = this.db(this.table).select('collection', 'name', 'data');

      colls.forEach(function (cName) {
        query.orWhereIn('name', requests[cName]).andWhere({ collection: cName });
      });

      query.exec(function (err, results) {
        if (err) return cb(err, null);

        results = results.reduce(function (obj, result) {
          obj[result.collection] = obj[result.collection] || {};
          obj[result.collection][result.name] = result.data;
          return obj;
        }, {});

        // Add colls with no documents found back to the results
        for (var i = 0, len = colls.length; i < len; i++) {
          if (!results[colls[i]]) results[colls[i]] = {};
        }

        cb(null, results);
      });
    }
  }, {
    key: 'writeOp',
    value: function writeOp(cName, docName, opData, cb) {
      this.db(this.table).insert({
        collection_name: cName,
        document_name: docName,
        version: opData.v,
        data: opData
      }).returning('data').exec(function (err) {
        if (err && err.code !== '23505') return cb(err, null);
        cb(null, opData);
      });
    }
  }, {
    key: 'getVersion',
    value: function getVersion(cName, docName, cb) {
      this.db(this.table).where({ collection_name: cName, document_name: docName }).select('version').orderBy('version', 'desc').limit(1).exec(function (err, rows) {
        if (err) return cb(err, null);
        cb(null, rows.length ? parseInt(rows[0].version, 10) + 1 : 0);
      });
    }
  }, {
    key: 'getOps',
    value: function getOps(cName, docName, start, end, cb) {
      var query = this.db(this.table).where('version', '>=', start).andWhere({ collection_name: cName, document_name: docName });

      if (typeof end === 'number') {
        query.andWhere('version', '<', end);
      }

      query.select('data').orderBy('version', 'asc').exec(function (err, rows) {
        if (err) return cb(err, null);
        cb(null, rows.map(function (row) {
          return row.data;
        }));
      });
    }
  }, {
    key: 'close',
    value: function close(cb) {
      LivePg.close(cb);
    }
  }], [{
    key: 'close',
    value: function close(cb) {
      if (this.willClose) {
        cb();
      } else {
        pg.once('end', cb);
        pg.end();
      }
    }
  }]);

  return LivePg;
})();

module.exports = LivePg;
