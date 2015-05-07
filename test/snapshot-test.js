'use strict';

const helper         = require('./test-helper');
const LivePg         = require('..');
const async          = require('async');
const should         = require('should');
const truncateTables = helper.truncateTables;

describe('LivePg (snapshots)', () => {
  let livePg;

  beforeEach(done => {
    livePg = new LivePg(process.env.PG_URL, 'documents');
    truncateTables(done);
  });

  afterEach(truncateTables);

  describe('#getSnapshot', () => {
    it('returns null when the document does not exist', done => {
      livePg.getSnapshot('collection', 'name', (err, doc) => {
        if (err) throw err;
        should(doc).equal(null);
        done();
      });
    });

    it('returns a document when it exists', done => {
      async.waterfall([
        cb => livePg.writeSnapshot('collection', 'name', { v: 1 }, cb),
        (_, cb) => livePg.getSnapshot('collection', 'name', cb)
      ], (err, doc) => {
        if (err) throw err;
        doc.should.eql({ v: 1 });
        done();
      });
    });
  });

  describe('#writeSnapshot', () => {
    it('inserts the document if it does not exist', done => {
      livePg.writeSnapshot('collection', 'name', { v: 1 }, (err, doc) => {
        if (err) throw err;
        doc.should.eql({ v: 1 });
        done();
      });
    });

    it('updates the document if it exists', done => {
      async.waterfall([
        cb => livePg.writeSnapshot('collection', 'name', { v: 1 }, cb),
        (_, cb) => livePg.writeSnapshot('collection', 'name', { v: 2 }, cb)
      ], (err, doc) => {
        if (err) throw err;
        doc.should.eql({ v: 2 });
        done();
      });
    });
  });

  describe('#bulkGetSnapshot', () => {
    it('returns all documents found', done => {
      async.waterfall([
        cb => livePg.writeSnapshot('collA', 'docA', { v: 1 }, cb),
        (_, cb) => livePg.writeSnapshot('collA', 'docB', { v: 2 }, cb),
        (_, cb) => livePg.writeSnapshot('collB', 'docC', { v: 2 }, cb),
        (_, cb) => livePg.writeSnapshot('collB', 'docD', { v: 2 }, cb),
        (_, cb) => {
          livePg.bulkGetSnapshot({
            collA: ['docA'],
            collB: ['docC', 'docD']
          }, cb);
        }
      ], (err, results) => {
        if (err) throw err;

        results.should.eql({
          collA: { docA: { v: 1 } },
          collB: { docC: { v: 2 }, docD: { v: 2 } }
        });

        done();
      });
    });

    it('returns empty objects for collections with no documents found', done => {
      livePg.bulkGetSnapshot({ coll: ['doc'] }, (err, results) => {
        if (err) throw err;
        results.should.eql({ coll: {} });
        done();
      });
    });

    it('does not return nonexistent documents', done => {
      async.waterfall([
        cb => livePg.writeSnapshot('collA', 'docA', { v: 1 }, cb),
        (_, cb) => livePg.writeSnapshot('collB', 'docB', { v: 2 }, cb),
        (_, cb) => {
          livePg.bulkGetSnapshot({
            collA: ['docA'],
            collB: ['docB', 'docC']
          }, cb);
        }
      ], (err, results) => {
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
