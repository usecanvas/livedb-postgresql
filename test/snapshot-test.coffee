
LivePg = require '..'
should = require 'should'

describe 'LivePg (snapshots)', ->
  beforeEach ->
    @livePg = new LivePg(process.env.PG_URL, 'documents')

  describe '#getSnapshot', ->
    it 'returns null when the document does not exist', (done) ->
      @livePg.getSnapshot 'collection', 'name', (err, doc) ->
        throw err if err
        should(doc).equal(null)
        done()
