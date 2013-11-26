#!/usr/bin/env node
var createClient = require('manta-client');
var mantaCouch = require('../');

var client = createClient();
var args = client.opts._args;

if (client.opts.help)
  return usage();

var db;
var path;
var seqFile;
var seq;
var inactivity_ms;

for (var i = 2; i < args.length; i++) {
  var arg = args[i];
  var val;
  if (arg.match(/^-Q/) || arg.match(/^--seq-file(=.+)?$/)) {
    if (arg.match(/^-Q.+$/))
      val = arg.slice(2);
    else if (arg.match(/^--seq-file=.+/))
      val = arg.slice('--seq-file='.length);
    else
      val = args[++i];
    if (!val) {
      console.error(arg + ' specified without value');
      usage();
      process.exit(1);
    }
    seqFile = val;
  } else if (arg.match(/^-q/) || arg.match(/--seq(\=.+)$/)) {
    if (arg.match(/^-q.+$/))
      val = arg.slice(2);
    else if (arg.match(/^--seq=.+/))
      val = arg.slice('--seq='.length);
    else
      val = args[++i];
    val = val && +val;
    if (!val && val !== 0) {
      console.error(arg + ' specified without numeric value');
      usage();
      process.exit(1);
    }
    seq = val;
  } else if (arg.match(/^--inactivity-ms(=.+)?$/)) {
    if (arg.match(/^--inactivity-ms=.+$/))
      val = arg.slice('--inactivity-ms='.length);
    else
      val = args[++i];
    val = val && +val;
    if (!val && val !== 0) {
      console.error(arg + ' specified without numeric value');
      usage();
      process.exit(1);
    }
    inactivity_ms = val;
  } else if (!db && !arg.match(/^-/))
    db = arg;
  else if (!path && !arg.match(/^-/))
    path = arg;
  else {
    console.error('unknown arg: ' + arg);
    usage();
    process.exit(1);
  }
}

if (!db || !path) {
  usage();
  process.exit(1);
}

function usage() {
  console.log(usage.toString().split(/\n/).slice(4, -2).join('\n'));
  console.log(createClient.parser.help());
/*
mcouch - Relax with the Fishes
Usage: mcouch [args] COUCHDB MANTAPATH

    COUCHDB                             Full url to your couch, like
                                        http://localhost:5984/database
    MANTAPATH                           Remote path in Manta, like
                                        ~~/stor/database
    -q --seq=SEQ                        Start at SEQ
    -Q --seq-file=FILE                  Store sequence number in FILE
    --inactivity-ms=NUM                 Restart if no activity in NUM ms
*/
}

mantaCouch({
  client: client,
  db: db,
  path: path,
  seqFile: seqFile,
  inactivity_ms: inactivity_ms,
  seq: seq
});
