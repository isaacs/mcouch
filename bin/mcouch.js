#!/usr/bin/env node
var createClient = require('manta-client');
var manta = require('manta');
var mantaCouch = require('../');
var dashdash = require('dashdash');
var parser = dashdash.createParser({
  options: [
    { names: [ 'seq-file', 'Q' ],
      type: 'string',
      help: 'File to store the sequence in',
      helpArg: 'FILE' },
    { names: [ 'seq', 'q' ],
      type: 'number',
      help: 'Sequence ID to start at',
      helpArg: 'NUMBER' },
    { names: [ 'inactivity-ms' ],
      type: 'number',
      help: 'Max ms to wait before assuming disconnection.',
      helpArg: 'MS' }
  ].concat(manta.DEFAULT_CLI_OPTIONS)
});

var opts = parser.parse(process.argv, process.env);
var args = opts._args;

if (opts.help || args.length !== 4)
  return usage();

var client = createClient(process.argv, process.env);
var db = args[2];
var path = args[3];
var seqFile = opts.seq_file;
var seq = opts.seq;
var inactivity_ms = opts.inactivity_ms;


if (!db || !path) {
  usage();
  process.exit(1);
}

function usage() {
  console.log(usage.toString().split(/\n/).slice(4, -2).join('\n'));
  console.log(parser.help());
/*
mcouch - Relax with the Fishes
Usage: mcouch [args] COUCHDB MANTAPATH

    COUCHDB                             Full url to your couch, like
                                        http://localhost:5984/database
    MANTAPATH                           Remote path in Manta, like
                                        ~~/stor/database
*/
}

var mc = mantaCouch({
  client: client,
  db: db,
  path: path,
  seqFile: seqFile,
  inactivity_ms: inactivity_ms,
  seq: seq
}).on('put', function(doc) {
  console.log('PUT %s', doc._id);
}).on('rm', function(doc) {
  console.log('RM %s', doc._id);
}).on('send', function(doc, file) {
  console.log('-> sent %s/%s', doc._id, file.name);
}).on('delete', function(doc, file) {
  console.log('-> deleted %s/%s', doc._id, file.name);
});
