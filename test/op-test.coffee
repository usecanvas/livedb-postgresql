
LivePg = require '..'
async  = require 'async'
should = require 'should'

require './test-helper'

opTable = 'doc.operations'
docTable = 'doc.documents'

describe 'LivePg (operations)', ->

  before (done) ->
    @livePg = new LivePg(process.env.PG_URL, opTable)
    @docPg  = new LivePg(process.env.PG_URL, docTable)
    done();

  after (done) ->
    async.parallel [
      @livePg.close,
      @docPg.close
    ], done

  beforeEach (done) ->
    async.parallel [
      ((cb) =>
        @livePg._query "TRUNCATE TABLE #{opTable}", cb
      ), ((cb) =>
        @docPg._query "TRUNCATE TABLE #{docTable}", cb
      )
    ], => @docPg.writeSnapshot 'coll', 'doc', { v: 1 }, done

  afterEach (done) ->
    async.parallel [
      ((cb) =>
        @livePg._query "TRUNCATE TABLE #{opTable}", cb
      ), ((cb) =>
        @docPg._query "TRUNCATE TABLE #{docTable}", cb
      )
    ], done

  describe '#writeOp', ->
    it 'returns the written op', (done) ->
      @livePg.writeOp 'coll', 'doc', { v: 1 }, (err, res) ->
        throw err if err
        res.should.eql v: 1
        done()

  describe '#getVersion', ->
    it 'returns 1 if there are no ops', (done) ->
      @livePg.getVersion 'coll', 'doc', (err, version) ->
        throw err if err
        version.should.equal 0
        done()

    it 'returns the next version of the document if there are ops', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.getVersion 'coll', 'doc', cb
        ), (version) ->
          version.should.eql 3
          done()
      ], (err) -> throw err

  describe '#getOps', ->
    it 'returns an empty array if there are no ops in the range', (done) ->
      @livePg.getOps 'coll', 'doc', 1, 2, (err, ops) ->
        throw err if err
        ops.should.eql []
        done()

    it 'does not return ops from other collections or docs', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeOp 'collA', 'doc', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'collB', 'docA', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'collB', 'doc', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'collB', 'doc', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.getOps 'collB', 'doc', 1, null, cb
        ), ((ops) ->
          ops.should.eql([{ v: 1 }, { v: 2 }])
          done()
        )
      ], (err) -> throw err

    it 'returns the ops, noninclusively', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 3 }, cb
        ), ((_, cb) =>
          @livePg.getOps 'coll', 'doc', 1, 3, cb
        ), ((ops) ->
          ops.should.eql([{ v: 1 }, { v: 2 }])
          done()
        )
      ], (err) -> throw err

    it 'returns the requested until the end if there is no end', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.writeOp 'coll', 'doc', { v: 3 }, cb
        ), ((_, cb) =>
          @livePg.getOps 'coll', 'doc', 2, null, cb
        ), ((ops) ->
          ops.should.eql([{ v: 2 }, { v: 3 }])
          done()
        )
      ], (err) -> throw err
