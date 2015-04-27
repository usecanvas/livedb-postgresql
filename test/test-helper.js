'use strict';

if (!process.env.PG_URL) {
  var url = 'postgres://localhost:5432/livedb-postgresql_test';
  process.stderr.write('Must provide $PG_URL such as ' + url + '\n\n');
  process.exit(1);
}

var async = require('async');
var knex  = require('knex');
var db    = knex({ client: 'pg', connection: process.env.PG_URL });

require('should');

exports.truncateTables = function truncateTables(cb) {
  async.parallel([
    function(cb) { truncateTable('operations', cb); },
    function(cb) { truncateTable('documents', cb); }
  ], cb);
};

function truncateTable(tableName, cb) {
  db.raw('TRUNCATE TABLE ' + tableName + ';').exec(cb);
}
