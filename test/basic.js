var client = require('./client.js')
var test = require('tap').test
var mpath = '~~/stor/mcouch-testing'
var mcouch = require('../mcouch.js')
var util = require('util');

test('first sync', function(t) {
  var evs = 'abcdef'.split('').reduce(function(s, a) {
    s.push(['put', a]);
    s.push(['sent', a + '/doc.json']);
    s.push(['complete', a]);
    return s;
  }, []).concat('ace'.split('').reduce(function (s, a) {
    s.push(['attachment', a + '/_attachments/binary.bin']);
    s.push(['attachment', a + '/_attachments/hello.txt']);
    s.push(['attachment', a + '/_attachments/world.txt']);
    s.push(['sent', a + '/_attachments/binary.bin']);
    s.push(['sent', a + '/_attachments/hello.txt']);
    s.push(['sent', a + '/_attachments/world.txt']);
    return s;
  }, []));

  testEvents(evs, t);
});

// Second time, only the text files get sent, because their
// digests are not trustworthy for our purposes.
test('second sync', function(t) {
  var evs = 'abcdef'.split('').reduce(function(s, a) {
    s.push(['put', a]);
    s.push(['complete', a]);
    return s;
  }, []).concat('ace'.split('').reduce(function (s, a) {
    s.push(['attachment', a + '/_attachments/hello.txt']);
    s.push(['attachment', a + '/_attachments/world.txt']);
    s.push(['sent', a + '/_attachments/hello.txt']);
    s.push(['sent', a + '/_attachments/world.txt']);
    return s;
  }, []));

  testEvents(evs, t);
});

function testEvents(evs, t) {
  evs = evs.reduce(function(set, e) {
    e = e[0] + ' ' + e[1];
    set[e] = (set[e] || 0) + 1
    return set;
  }, {})

  function ev() {
    var s = util.format.apply(util, arguments);
    t.ok(evs[s], s);
    evs[s]--;
    if (evs[s] === 0)
      delete evs[s];
    if (Object.keys(evs).length === 0) {
      mc.destroy();
      t.end();
    }
  }

  var mc = mcouch({
    debug: true,
    client: client,
    db: 'http://localhost:15984/mcouch',
    path: '~~/stor/mcouch-testing',
    seq: 0,
    inactivity_ms: 10000
  }).on('put', function(change) {
    ev('put %s', change.id);
  }).on('rm', function(change) {
    ev('rm %s', change.id);
  }).on('send', function(change, file) {
    ev('sent %s/%s', change.id, file.name);
  }).on('delete', function(change, file) {
    ev('delete %s/%s', change.id, file.name);
  }).on('attachment', function(change, file) {
    ev('attachment %s/%s', change.id, file.name);
  }).on('complete', function(change, results) {
    ev('complete %s', change.id);
  });
}
