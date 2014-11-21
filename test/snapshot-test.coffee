
LivePg = require '..'
should = require 'should'

describe 'LivePg (snapshots)', ->
  beforeEach (done) ->
    @livePg = new LivePg(process.env.PG_URL, 'documents')
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
      @livePg.writeSnapshot 'collection', 'name', { v: 1 }, (err, doc) =>
        throw err if err
        @livePg.getSnapshot 'collection', 'name', (err, doc) =>
          throw err if err
          doc.should.eql v: 1
          done()

  describe '#writeSnapshot', ->
    it 'inserts the document if it does not exist', (done) ->
      @livePg.writeSnapshot 'collection', 'name', { v: 1 }, (err, doc) ->
        throw err if err
        doc.should.eql v: 1
        done()

    it 'updates the document if it exists', (done) ->
      @livePg.writeSnapshot 'collection', 'name', { v: 1 }, (err, doc) =>
        throw err if err
        @livePg.writeSnapshot 'collection', 'name', { v: 2 }, (err, doc) =>
          throw err if err
          doc.should.eql v: 2
          done()
