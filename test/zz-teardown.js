// kill the couchdb process that's running as a detached child process
// started by the 00-setup.js test

var fs = require('fs')
var test = require('tap').test
var path = require('path')
var pidfile = path.resolve(__dirname, 'fixtures', 'pid')
var _users = path.resolve(__dirname, 'fixtures', '_users.couch')
var db = path.resolve(__dirname, 'fixtures', 'mcouch.couch')

test('kill all the users', function (t) {
  t.pass('_users db deleted')
  t.end()
})

test('craigslist (well, how do you get rid of YOUR couches?)', function (t) {
  try {
    var pid = fs.readFileSync(pidfile)
  } catch (er) {}
  if (pid) {
    fs.unlinkSync(pidfile)
    try { process.kill(pid) } catch (er) {
      // ok if already killed
      t.equal(er.code, 'ESRCH')
    }
  }
  try { fs.unlinkSync(_users) } catch (er) {
    // ok if gone
    t.equal(er.code, 'ENOENT')
  }
  try { fs.unlinkSync(db) } catch (er) {
    // ok if gone
    t.equal(er.code, 'ENOENT')
  }
  t.pass('couch is no more')
  t.end()
})

test('cleanup fishes', function(t) {
  var client = require('./client.js')
  client.rmr('~~/stor/mcouch-testing', function(er) {
    if (er && er.statusCode !== 404)
      throw er
    t.pass('done')
    t.end()
    client.close()
  })
})
