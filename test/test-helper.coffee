pg = require('pg')
pg.defaults.poolSize = 100;

if !process.env.PG_URL
  url = 'postgres://localhost:5432/livedb-postgresql_test'
  process.stderr.write 'Must provide $PG_URL such as ' + url + '\n\n'
  process.exit 1
