# livedb-postgresql

This is a PostgreSQL adapter for [livedb][livedb]. It does not yet implement the
livedb query interface.

**Please exercise caution** when using this package. It's *mostly* but not
thoroughly tested.

See [schema.sql][schema] for an example database schema that works with this package.

## Install

```sh
npm install --save livedb-postgresql
```

## Usage

```javascript
var LivePg = require('livedb-postgresql');
var livedb = require('livedb');
var redis  = require('redis');

// Redis clients
var redisURL  = require('url').parse(process.env.REDIS_URL);
var redisPass = url.auth.split(':')[1];
var redis1    = redis.createClient(url.port, url.hostname, { auth_pass: redisPass });
var redis2    = redis.createClient(url.port, url.hostname, { auth_pass: redisPass });

// Postgres clients
var connString = process.env.DATABASE_URL;
var snapshotDb = new LivePg(connString, 'documents');  // "documents" is a table
var opLog      = new LivePg(connString, 'oplog');

var driver     = livedb.redisDriver(opLog, redis1, redis2);
var liveClient = livedb.client({ snapshotDb: snapshotDb, driver: driver });
```

[livedb]: https://github.com/share/livedb
[schema]: https://github.com/slowink/livedb-postgresql/blob/master/schema.sql
