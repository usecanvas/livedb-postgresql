'use strict';

if (!process.env.PG_URL) {
  const url = 'postgres://localhost:5432/livedb-postgresql_test';
  process.stderr.write(`Must provide $PG_URL such as ${url}\n\n`);
  process.exit(1);
}

const async = require('async');
const knex  = require('knex');
const db    = knex({ client: 'pg', connection: process.env.PG_URL });

require('should');

exports.truncateTables = cb => {
  async.parallel([
    cb => truncateTable('operations', cb),
    cb => truncateTable('documents', cb)
  ], cb);
};

function truncateTable(tableName, cb) {
  db.raw('TRUNCATE TABLE ' + tableName + ';').exec(cb);
}
