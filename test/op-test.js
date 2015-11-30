'use strict';

var helper         = require('./test-helper');
var LivePg         = require('..');
var async          = require('async');
var truncateTables = helper.truncateTables;

describe('LivePg (operations)', function() {
  var docPg, livePg;

  beforeEach(function(done) {
    livePg = new LivePg.OpLog({
      conn            : process.env.PG_URL,
      table           : 'operations',
      collectionColumn: 'collection_name',
      documentColumn  : 'document_name',
      dataColumn      : 'data',
      versionColumn   : 'version',
    });

    docPg  = new LivePg.Snapshots({
      conn            : process.env.PG_URL,
      table           : 'documents',
      collectionColumn: 'collection_name',
      nameColumn      : 'name',
      dataColumn      : 'data',
    });

    truncateTables(function() {
      docPg.writeSnapshot('coll', 'doc', { v: 1 }, done);
    });
  });

  afterEach(function(done) {
    truncateTables(done);
  });

  describe('#writeOp', function() {
    it('returns the written op', function(done) {
      livePg.writeOp('coll', 'doc', { v: 1 }, function(err, res) {
        if (err) throw err;
        res.should.eql({ v: 1 });
        done();
      });
    });

    it('ignores duplicate ops', function(done) {
      livePg.writeOp('coll', 'doc', { v: 1 }, function(err) {
        if (err) throw err;

        livePg.writeOp('coll', 'doc', { v: 1 }, function(err, res) {
          if (err) throw err;
          res.should.eql({ v: 1 });
          done();
        });
      });
    });
  });

  describe('#getVersion', function() {
    it('returns 0 if there are no ops', function(done) {
      livePg.getVersion('coll', 'doc', function(err, version) {
        if (err) throw err;
        version.should.eql(0);
        done();
      });
    });

    it('returns the next version of the document if there are ops', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeOp('coll', 'doc', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('coll', 'doc', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.getVersion('coll', 'doc', cb);
        }
      ], function(err, version) {
        if (err) throw err;
        version.should.eql(3);
        done();
      });
    });
  });

  describe('#getOps', function() {
    it('returns an empty array if there are no ops in the range', function(done) {
      livePg.getOps('coll', 'doc', 1, 2, function(err, ops) {
        if (err) throw err;
        ops.should.eql([]);
        done();
      });
    });

    it('does not return ops from other collections or docs', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeOp('collA', 'doc', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('collB', 'docA', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('collB', 'doc', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('collB', 'doc', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.getOps('collB', 'doc', 1, null, cb);
        }
      ], function(err, ops) {
        if (err) throw err;
        ops.should.eql([{ v: 1 }, { v: 2 }]);
        done();
      });
    });

    it('returns the ops, noninclusively', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeOp('coll', 'doc', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('coll', 'doc', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('coll', 'doc', { v: 3 }, cb);
        },

        function(_, cb) {
          livePg.getOps('coll', 'doc', 1, 3, cb);
        }
      ], function(err, ops) {
        if (err) throw err;
        ops.should.eql([{ v: 1 }, { v: 2 }]);
        done();
      });
    });

    it('returns the requested until the end if there is no end', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeOp('coll', 'doc', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('coll', 'doc', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.writeOp('coll', 'doc', { v: 3 }, cb);
        },

        function(_, cb) {
          livePg.getOps('coll', 'doc', 2, null, cb);
        }
      ], function(err, ops) {
        if (err) throw err;
        ops.should.eql([{ v: 2 }, { v: 3 }]);
        done();
      });
    });
  });
});
