'use strict';

var helper         = require('./test-helper');
var LivePg         = require('..');
var async          = require('async');
var should         = require('should');
var truncateTables = helper.truncateTables;

describe('LivePg (snapshots)', function() {
  var livePg;

  beforeEach(function(done) {
    livePg = new LivePg({ conn: process.env.PG_URL, table: 'documents' });
    truncateTables(done);
  });

  afterEach(truncateTables);

  describe('#getSnapshot', function() {
    it('returns null when the document does not exist', function(done) {
      livePg.getSnapshot('collection', 'name', function(err, doc) {
        if (err) throw err;
        should(doc).equal(null);
        done();
      });
    });

    it('returns a document when it exists', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeSnapshot('collection', 'name', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.getSnapshot('collection', 'name', cb);
        }
      ], function(err, doc) {
        if (err) throw err;
        doc.should.eql({ v: 1 });
        done();
      });
    });
  });

  describe('#writeSnapshot', function() {
    it('inserts the document if it does not exist', function(done) {
      livePg.writeSnapshot('collection', 'name', { v: 1 }, function(err, doc) {
        if (err) throw err;
        doc.should.eql({ v: 1 });
        done();
      });
    });

    it('updates the document if it exists', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeSnapshot('collection', 'name', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeSnapshot('collection', 'name', { v: 2 }, cb);
        }
      ], function(err, doc) {
        if (err) throw err;
        doc.should.eql({ v: 2 });
        done();
      });
    });
  });

  describe('#bulkGetSnapshot', function() {
    it('returns all documents found', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeSnapshot('collA', 'docA', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeSnapshot('collA', 'docB', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.writeSnapshot('collB', 'docC', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.writeSnapshot('collB', 'docD', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.bulkGetSnapshot({
            collA: ['docA'],
            collB: ['docC', 'docD']
          }, cb);
        }
      ], function(err, results) {
        if (err) throw err;

        results.should.eql({
          collA: { docA: { v: 1 } },
          collB: { docC: { v: 2 }, docD: { v: 2 } }
        });

        done();
      });
    });

    it('returns empty objects for collections with no documents found', function(done) {
      livePg.bulkGetSnapshot({ coll: ['doc'] }, function(err, results) {
        if (err) throw err;
        results.should.eql({ coll: {} });
        done();
      });
    });

    it('does not return nonexistent documents', function(done) {
      async.waterfall([
        function(cb) {
          livePg.writeSnapshot('collA', 'docA', { v: 1 }, cb);
        },

        function(_, cb) {
          livePg.writeSnapshot('collB', 'docB', { v: 2 }, cb);
        },

        function(_, cb) {
          livePg.bulkGetSnapshot({
            collA: ['docA'],
            collB: ['docB', 'docC']
          }, cb);
        }
      ], function(err, results) {
        if (err) throw err;

        results.should.eql({
          collA: { docA: { v: 1 } },
          collB: { docB: { v: 2 } }
        });

        done();
      });
    });
  });
});
