LivePg = require '..'
async  = require 'async'
should = require 'should'

describe 'LivePg (operations)', ->
  beforeEach (done) ->
    @livePg = new LivePg(process.env.PG_URL, 'operations')
    @docPg  = new LivePg(process.env.PG_URL, 'documents')

    async.parallel [
      ((cb) =>
        @livePg.db.raw('TRUNCATE TABLE operations').exec cb
      ), ((cb) =>
        @docPg.db.raw('TRUNCATE TABLE documents').exec cb
      )
    ], =>
      @docPg.writeSnapshot 'coll', 'doc', { v: 1 }, done

  afterEach (done) ->
    async.parallel [
      ((cb) =>
        @livePg.db.raw('TRUNCATE TABLE operations').exec cb
      ), ((cb) =>
        @docPg.db.raw('TRUNCATE TABLE documents').exec cb
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
        version.should.equal 1
        done()

    it 'returns the next version of the document if there are ops', (done) ->
      @livePg.writeOp 'coll', 'doc', { v: 1 }, (err, res) =>
        throw err if err
        @livePg.writeOp 'coll', 'doc', { v: 2 }, (err, res) =>
          throw err if err
          @livePg.getVersion 'coll', 'doc', (err, version) ->
            throw err if err
            version.should.equal 3
            done()

