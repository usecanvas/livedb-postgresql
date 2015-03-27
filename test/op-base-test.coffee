
# This is a mocha test suite for oplog implementations
#
#
# getOps collection, docName, start, end
assert = require 'assert'
async = require 'async'
textType = require('ot-text').type
LivePg = require '..'

require './test-helper'

opTable = 'doc.operations'
docTable = 'doc.documents'

# Wait for the returned function to be called a given number of times, then call the
# callback.
makePassPart = (n, callback) ->
  remaining = n
  ->
    remaining--
    if remaining == 0
      callback()
    else if remaining < 0
      throw new Error "expectCalls called more than #{n} times"

counter = 1

describe 'oplog', ->

  before (done) ->
    @docName = "doc #{counter++}"
    @cName = 'coll'
    @opPg = new LivePg(process.env.PG_URL, opTable)
    done();

  after (done) ->
    @opPg.close(done)

  beforeEach (done) ->
    @opPg._query "TRUNCATE TABLE #{opTable}", done

  afterEach (done) ->
    @opPg._query "TRUNCATE TABLE #{opTable}", done

  it 'returns 0 when getVersion is called on a new document', (done) ->
    @opPg.getVersion @cName, @docName, (err, v) ->
      throw new Error err if err
      assert.strictEqual v, 0
      done()

  it 'writing an operation bumps the version', (done) ->
    @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err, res) =>
      throw new Error err if err
      @opPg.getVersion @cName, @docName, (err, v) =>
        throw new Error err if err
        assert.strictEqual v, 1
        @opPg.writeOp @cName, @docName, {v:1, op:['hi']}, (err) =>
          @opPg.getVersion @cName, @docName, (err, v) ->
            throw new Error err if err
            assert.strictEqual v, 2
            done()

  it 'ignores subsequent attempts to write the same operation', (done) ->
    @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err) =>
      throw new Error err if err
      @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err) =>
        throw new Error err if err

        @opPg.getVersion @cName, @docName, (err, v) =>
          throw new Error err if err
          assert.strictEqual v, 1
          @opPg.getOps @cName, @docName, 0, null, (err, ops) ->
            assert.strictEqual ops.length, 1
            done()

  it 'does not decrement the version when receiving old ops', (done) ->
    @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err) =>
      throw new Error err if err
      @opPg.writeOp @cName, @docName, {v:1, op:['hi']}, (err) =>
        throw new Error err if err
        @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err) =>
          throw new Error err if err
          @opPg.getVersion @cName, @docName, (err, v) =>
            throw new Error err if err
            assert.strictEqual v, 2
            done()

  it 'ignores concurrent attempts to write the same operation', (done) ->
    @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err) =>
      throw new Error err if err
    @opPg.writeOp @cName, @docName, {v:0, create:{type:textType.uri}}, (err) =>
      throw new Error err if err

      @opPg.getVersion @cName, @docName, (err, v) =>
        throw new Error err if err
        assert.strictEqual v, 1
        @opPg.getOps @cName, @docName, 0, null, (err, ops) ->
          assert.strictEqual ops.length, 1
          done()

  describe 'getOps', ->
    it 'returns [] for a nonexistant document, with any arguments', (done) ->
      num = 0
      check = (error, ops) ->
        throw new Error error if error
        assert.deepEqual ops, []
        done() if ++num is 7

      @opPg.getOps @cName, @docName, 0, 0, check
      @opPg.getOps @cName, @docName, 0, 1, check
      @opPg.getOps @cName, @docName, 0, 10, check
      @opPg.getOps @cName, @docName, 0, null, check
      @opPg.getOps @cName, @docName, 10, 10, check
      @opPg.getOps @cName, @docName, 10, 11, check
      @opPg.getOps @cName, @docName, 10, null, check

    it 'returns ops', (done) ->
      num = 0
      check = (expected) -> (error, ops) ->
        throw new Error error if error
        if ops then delete op.v for op in ops
        assert.deepEqual ops, expected
        done() if ++num is 5

      opData = {v:0, op:[{p:0,i:'hi'}], meta:{}, src:'abc', seq:123}
      @opPg.writeOp @cName, @docName, opData, =>
        delete opData.v
        @opPg.getOps @cName, @docName, 0, 0, check []
        @opPg.getOps @cName, @docName, 0, 1, check [opData]
        @opPg.getOps @cName, @docName, 0, null, check [opData]
        @opPg.getOps @cName, @docName, 1, 1, check []
        @opPg.getOps @cName, @docName, 1, null, check []
