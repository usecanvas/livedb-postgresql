
# This is a test suite for snapshot database implementations
assert = require 'assert'
textType = require('ot-text').type
jsonType = require('ot-json0').type
LivePg = require '..'

require './test-helper'

opTable = 'doc.operations'
docTable = 'doc.documents'

counter = 1

describe 'snapshot db', ->

  before (done) ->
    @docName = "doc #{counter++}"
    @cName = 'coll'
    @docPg = new LivePg(process.env.PG_URL, docTable)
    done();

  after (done) ->
    @docPg.close(done)

  beforeEach (done) ->

    @docPg.db.raw("TRUNCATE TABLE #{docTable}").exec () ->
      done()

  afterEach (done) ->

    @docPg.db.raw("TRUNCATE TABLE #{docTable}").exec () ->
      done()

  it 'returns null when you getSnapshot on a nonexistant doc name', (done) ->
    @docPg.getSnapshot @cName, @docName, (err, data) ->
      throw Error(err) if err
      assert.equal data, null
      done()

  it 'will store data', (done) ->
    data = {v:5, type:textType.uri, data:'hi there', m:{ctime:1, mtime:2}}
    @docPg.writeSnapshot @cName, @docName, data, (err) =>
      throw Error(err) if err
      @docPg.getSnapshot @cName, @docName, (err, storedData) ->
        delete storedData.docName # The result is allowed to contain this but its ignored.
        assert.deepEqual data, storedData
        done()

  it 'will remove data fields if the data has been deleted', (done) ->
    data = {v:5, type:textType.uri, data:'hi there', m:{ctime:1, mtime:2}}
    @docPg.writeSnapshot @cName, @docName, data, (err) =>
      throw Error(err) if err
      @docPg.writeSnapshot @cName, @docName, {v:6}, (err) =>
        throw Error(err) if err
        @docPg.getSnapshot @cName, @docName, (err, storedData) ->
          throw Error(err) if err
          assert.equal storedData.data, null
          assert.equal storedData.type, null
          assert.equal storedData.m, null
          assert.equal storedData.v, 6
          done()

  it 'does not return missing documents', (done) ->
    request = {}
    request[@cName] = [@docName]

    expected = {}
    expected[@cName] = []

    @docPg.bulkGetSnapshot request, (err, results) ->
      throw Error(err) if err
      assert.deepEqual results, expected
      done()

  it 'returns results', (done) ->
    data = {v:5, type:textType.uri, data:'hi there', m:{ctime:1, mtime:2}}

    request = {}
    request[@cName] = [@docName]

    expected = {}
    expected[@cName] = {}

    @docPg.writeSnapshot @cName, @docName, data, (err) =>
      throw Error(err) if err
      @docPg.bulkGetSnapshot request, (err, results) =>
        throw Error(err) if err
        expected[@cName][@docName] = data
        delete results[@cName][@docName].docName
        assert.deepEqual results, expected
        done()

  it "works when some results exist and some don't", (done) ->
    data = {v:5, type:textType.uri, data:'hi there', m:{ctime:1, mtime:2}}

    request = {}
    request[@cName] = ['does not exist', @docName, 'also does not exist']

    expected = {}
    expected[@cName] = {}

    @docPg.writeSnapshot @cName, @docName, data, (err) =>
      throw Error(err) if err
      @docPg.bulkGetSnapshot request, (err, results) =>
        throw Error(err) if err
        expected[@cName][@docName] = data
        delete results[@cName][@docName].docName
        assert.deepEqual results, expected
        done()

  it 'projects fields using getSnapshotProjected', (done) ->
    if !@docPg.getSnapshotProjected
      console.warn 'No getSnapshotProjected implementation. Skipping tests. This is ok - it just means projections will be less efficient'
      return done()

    data = {v:5, type:jsonType.uri, data:{x:5, y:6}, m:{ctime:1, mtime:2}}
    @docPg.writeSnapshot @cName, @docName, data, (err) =>
      throw Error err if err

      @docPg.getSnapshotProjected @cName, @docName, {x:true, z:true}, (err, data) ->
        throw Error err if err
        delete data.docName
        expected = {v:5, type:jsonType.uri, data:{x:5}, m:{ctime:1, mtime:2}}
        assert.deepEqual data, expected
        done()
