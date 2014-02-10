var client = require('./client.js');
var test = require('tap').test;
var mpath = '~~/stor/mcouch-testing/forensic';
var parse = require('parse-json-response');
var mcouch = require('../mcouch.js');
var util = require('util');
var seq = 0;

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

// Make some deletes and changes
test('make changes and deletes', function(t) {
  var http = require('http');
  var url = require('url');

  var h = url.parse('http://localhost:15984/mcouch/a');
  h.method = 'HEAD';
  h.headers = { connection: 'close' };
  http.request(h, function(res) {
    var rev = res.headers.etag.replace(/^"|"$/g, '');
    var d = url.parse('http://localhost:15984/mcouch/a?rev=' + rev);
    d.method = 'DELETE';
    d.headers = { connection: 'close' };
    http.request(d, function(res) {
      then();
    }).end();
  }).end();

  http.get('http://localhost:15984/mcouch/b', parse(function(er, data) {
    if (er)
      throw er;

    data.didPut = true;
    b = new Buffer(JSON.stringify(data));
    var p = url.parse('http://localhost:15984/mcouch/b');
    p.method = 'PUT';
    p.headers = {
      'content-type':'application/json',
      'content-length': b.length,
      connection: 'close'
    };
    http.request(p, function(res) {
      then();
    }).end(b);
  }));

  var n = 0;
  function then() {
    if (++n === 1)
      return
    t.pass('did writes');
  }

  var evs = [
    ['put', 'b'],
    ['sent', 'b/doc.json'],
    ['complete', 'b'],
    ['rm', 'a']
  ];

  testEvents(evs, t, true);
});

function testEvents(evs, t, useSeq) {
  evs = evs.reduce(function(set, e) {
    e = e[0] + ' ' + e[1];
    set[e] = (set[e] || 0) + 1
    return set;
  }, {})

  function ev() {
    seq = mc.seq;
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
    path: mpath,
    seq: useSeq ? seq : 0,
    inactivity_ms: 10000,
    forensic: true
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

test('verify forensic results', function(t) {
  var spawn = require('child_process').spawn;
  var expect = [
    'a/_deleted-0/4-f7f10777ae54d9f27b3c86604339de33.json$',
    'a/_deleted-0/_attachments/binary.bin$',
    'a/_deleted-0/_attachments/binary.bin-5d41402abc4b2a76b9719d911017c592$',
    'a/_deleted-0/_attachments/hello.txt$',
    'a/_deleted-0/_attachments/hello.txt-6f8d1fefa54ab20698b249867d66036e$',
    'a/_deleted-0/_attachments/world.txt$',
    'a/_deleted-0/_attachments/world.txt-88c6a20bcc2a885943d8d8cb4de9af09$',
    'a/_deleted-0/doc.json$',
    'a/doc.json$',
    'b/1-bc73c56b805ca79ca32135c0da81c1d3.json$',
    'b/2-c8ea5947521c859e800f365d4cffd557.json$',
    'b/doc.json$',
    'c/4-1ed4c9892bd1a6bd604beb5ecb2c435a.json$',
    'c/_attachments/binary.bin$',
    'c/_attachments/binary.bin-5d41402abc4b2a76b9719d911017c592$',
    'c/_attachments/hello.txt$',
    'c/_attachments/hello.txt-6f8d1fefa54ab20698b249867d66036e$',
    'c/_attachments/world.txt$',
    'c/_attachments/world.txt-88c6a20bcc2a885943d8d8cb4de9af09$',
    'c/doc.json$',
    'd/1-11bec46da36229705d35f0219d863536.json$',
    'd/doc.json$',
    'e/4-3a05b26a2c7471655b5c0d38e62c7386.json$',
    'e/_attachments/binary.bin$',
    'e/_attachments/binary.bin-5d41402abc4b2a76b9719d911017c592$',
    'e/_attachments/hello.txt$',
    'e/_attachments/hello.txt-6f8d1fefa54ab20698b249867d66036e$',
    'e/_attachments/world.txt$',
    'e/_attachments/world.txt-88c6a20bcc2a885943d8d8cb4de9af09$',
    'e/doc.json$',
    'f/1-64539941ca93602c848706f60ea9693d.json$',
    'f/doc.json$'
  ];
  var out = '';
  var c = spawn('mfind', [ '-t', 'o', mpath ]);
  c.stdout.setEncoding('utf8');
  c.stdout.on('data', function(d) {
    out += d;
  });
  c.stdout.on('end', function() {
    out = out.trim().split('\n').sort();
    out.forEach(function(line, i) {
      t.like(line, new RegExp(expect[i]));
    });
    t.end();
  });
});
