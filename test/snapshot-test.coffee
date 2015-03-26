LivePg = require '..'
async  = require 'async'
should = require 'should'

require './test-helper'

describe 'LivePg (snapshots)', ->

  before (done) ->
    @livePg = new LivePg(process.env.PG_URL, 'documents')
    done()

  after (done) ->
    @livePg.close(done)

  beforeEach (done) ->
    @livePg.db.raw('TRUNCATE TABLE documents').exec done

  afterEach (done) ->
    @livePg.db.raw('TRUNCATE TABLE documents').exec done

  describe '#getSnapshot', ->
    it 'returns null when the document does not exist', (done) ->
      @livePg.getSnapshot 'collection', 'name', (err, doc) ->
        throw err if err
        should(doc).equal(null)
        done()

    it 'returns a document when it exists', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeSnapshot 'collection', 'name', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.getSnapshot 'collection', 'name', cb
        ), (doc) ->
          doc.should.eql v: 1
          done()
      ], (err) -> throw err

  describe '#writeSnapshot', ->
    it 'inserts the document if it does not exist', (done) ->
      @livePg.writeSnapshot 'collection', 'name', { v: 1 }, (err, doc) ->
        throw err if err
        doc.should.eql v: 1
        done()

    it 'updates the document if it exists', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeSnapshot 'collection', 'name', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeSnapshot 'collection', 'name', { v: 2 }, cb
        ), (doc) ->
          doc.should.eql v: 2
          done()
      ], (err) -> throw err

  describe '#bulkGetSnapshot', ->
    it 'returns all documents found', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeSnapshot 'collA', 'docA', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeSnapshot 'collA', 'docB', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.writeSnapshot 'collB', 'docC', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.writeSnapshot 'collB', 'docD', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.bulkGetSnapshot { collA: ['docA'], collB: ['docC', 'docD'] }, cb
        ), (results) ->
          results.should.eql({
            collA: { docA: { v: 1 } },
            collB: { docC: { v: 2 }, docD: { v: 2 } }
          })
          done()
      ], (err) -> throw err

    it 'returns empty objects for collections with no documents found', (done) ->
      @livePg.bulkGetSnapshot { coll: ['doc'] }, (err, results) ->
        throw err if err
        results.should.eql { coll: {} }
        done()

    it 'does not return nonexistent documents', (done) ->
      async.waterfall [
        ((cb) =>
          @livePg.writeSnapshot 'collA', 'docA', { v: 1 }, cb
        ), ((_, cb) =>
          @livePg.writeSnapshot 'collB', 'docB', { v: 2 }, cb
        ), ((_, cb) =>
          @livePg.bulkGetSnapshot { collA: ['docA'], collB: ['docB', 'docC'] }, cb
        ), (results) ->
          results.should.eql({
            collA: { docA: { v: 1 } },
            collB: { docB: { v: 2 } }
          })
          done()
      ], (err) -> throw err
