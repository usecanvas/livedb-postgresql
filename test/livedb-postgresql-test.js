'use strict';

var LivePg = require('..');
var async  = require('async');
var pg     = require('pg');
var sinon  = require('sinon');

require('./test-helper');

describe('LivePg', function() {
  var livePg;

  beforeEach(function() {
    livePg = new LivePg({ conn: process.env.PG_URL, table: 'documents' });
  });

  describe('#close', function() {
    beforeEach(function() {
      LivePg.willClose = undefined;
      sinon.spy(pg, 'end');
    });

    afterEach(function() {
      pg.end.restore();
    });

    it('calls end on pg', function(done) {
      livePg.close(function() {
        pg.end.callCount.should.eql(1);
        done();
      });
    });

    it('does not call end on pg twice', function(done) {
      async.series([
        function(cb) { livePg.close(cb); },
        function(cb) { livePg.close(cb); }
      ], function(err) {
        if (err) throw err;
        pg.end.callCount.should.eql(1);
        done();
      });
    });

    it('does not call end on pg if it has already been called', function(done) {
      pg.end();
      livePg.close(function() {
        pg.end.callCount.should.eql(1);
        done();
      });
    });
  });
});
