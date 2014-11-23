# livedb-postgresql

This is a PostgreSQL adapter for [livedb][livedb]. It does not yet implement the
livedb query interface.

## Install

```sh
npm install --save livedb-postgresql
```

## Schema

### Requirements

livedb-postgresql has relatively relaxed requirements for the database it connects to. The table names can be anything, as they're set when creating an instance of livedb-postgresql.

#### Snapshots Table

| Column Name | Type |
|-------------|------|
| collection  | text |
| name        | text |
| data        | json |

#### Operations Table

| Column Name     | Type   |
|-----------------|--------|
| collection_name | text   |
| document_name   | text   |
| version         | bigint |
| data            | json   |

### Example

Here is an example SQL statement that will work with livedb-postgresql:

```sql
CREATE TABLE documents(
  collection text NOT NULL,
  name text NOT NULL,
  data json NOT NULL
);

CREATE UNIQUE INDEX documents_collection_name ON documents(collection, name);

CREATE TABLE operations(
  collection_name text NOT NULL,
  document_name text NOT NULL,
  version bigint NOT NULL,
  data json NOT NULL
);

CREATE UNIQUE INDEX operations_cname_docname_version ON operations(collection_name, document_name, version);
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

## Testing

After creating database tables such as the ones in [schema.sql][schema]:

```sh
PG_URL=postgres://localhost:5432/livedb-postgresql_test npm test
```

[livedb]: https://github.com/share/livedb
[schema]: https://github.com/slowink/livedb-postgresql/blob/master/schema.sql
