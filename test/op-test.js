'use strict';

const helper         = require('./test-helper');
const LivePg         = require('..');
const async          = require('async');
const truncateTables = helper.truncateTables;

describe('LivePg (operations)', () => {
  let docPg, livePg;

  beforeEach(done => {
    livePg = new LivePg({
      conn : process.env.PG_URL,
      table: 'operations' });

    docPg  = new LivePg({
      conn : process.env.PG_URL,
      table: 'documents' });

    truncateTables(() => {
      docPg.writeSnapshot('coll', 'doc', { v: 1 }, done);
    });
  });

  afterEach(done => {
    truncateTables(done);
  });

  describe('#writeOp', () => {
    it.only('returns the written op', done => {
      livePg.writeOp('coll', 'doc', { v: 1 }, (err, res) => {
        if (err) throw err;
        res.should.eql({ v: 1 });
        done();
      });
    });

    it('ignores duplicate ops', done => {
      livePg.writeOp('coll', 'doc', { v: 1 }, err => {
        if (err) throw err;

        livePg.writeOp('coll', 'doc', { v: 1 }, (err, res) => {
          if (err) throw err;
          res.should.eql({ v: 1 });
          done();
        });
      });
    });
  });

  describe('#getVersion', () => {
    it('returns 0 if there are no ops', done => {
      livePg.getVersion('coll', 'doc', (err, version) => {
        if (err) throw err;
        version.should.eql(0);
        done();
      });
    });

    it('returns the next version of the document if there are ops', done => {
      async.waterfall([
        cb => livePg.writeOp('coll', 'doc', { v: 1 }, cb),
        (_, cb) => livePg.writeOp('coll', 'doc', { v: 2 }, cb),
        (_, cb) => livePg.getVersion('coll', 'doc', cb)
      ], (err, version) => {
        if (err) throw err;
        version.should.eql(3);
        done();
      });
    });
  });

  describe('#getOps', () => {
    it('returns an empty array if there are no ops in the range', done => {
      livePg.getOps('coll', 'doc', 1, 2, (err, ops) => {
        if (err) throw err;
        ops.should.eql([]);
        done();
      });
    });

    it('does not return ops from other collections or docs', done => {
      async.waterfall([
        cb => livePg.writeOp('collA', 'doc', { v: 1 }, cb),
        (_, cb) => livePg.writeOp('collB', 'docA', { v: 1 }, cb),
        (_, cb) => livePg.writeOp('collB', 'doc', { v: 1 }, cb),
        (_, cb) => livePg.writeOp('collB', 'doc', { v: 2 }, cb),
        (_, cb) => livePg.getOps('collB', 'doc', 1, null, cb)
      ], (err, ops) => {
        if (err) throw err;
        ops.should.eql([{ v: 1 }, { v: 2 }]);
        done();
      });
    });

    it('returns the ops, noninclusively', done => {
      async.waterfall([
        cb => livePg.writeOp('coll', 'doc', { v: 1 }, cb),
        (_, cb) => livePg.writeOp('coll', 'doc', { v: 2 }, cb),
        (_, cb) => livePg.writeOp('coll', 'doc', { v: 3 }, cb),
        (_, cb) => livePg.getOps('coll', 'doc', 1, 3, cb)
      ], (err, ops) => {
        if (err) throw err;
        ops.should.eql([{ v: 1 }, { v: 2 }]);
        done();
      });
    });

    it('returns the requested until the end if there is no end', done => {
      async.waterfall([
        cb => livePg.writeOp('coll', 'doc', { v: 1 }, cb),
        (_, cb) => livePg.writeOp('coll', 'doc', { v: 2 }, cb),
        (_, cb) => livePg.writeOp('coll', 'doc', { v: 3 }, cb),
        (_, cb) => livePg.getOps('coll', 'doc', 2, null, cb)
      ], (err, ops) => {
        if (err) throw err;
        ops.should.eql([{ v: 2 }, { v: 3 }]);
        done();
      });
    });
  });
});
