module.exports = MantaCouch;

var follow = require('follow');
var url = require('url');
var crypto = require('crypto');
var assert = require('assert');

var SeqFile = require('seq-file');

var parse = require('parse-json-response');

var cuttlefish = require('cuttlefish');

var path = require('path');
var fs = require('fs');
var PassThrough = require('stream').PassThrough;
var EE = require('events').EventEmitter;
var util = require('util');

if (!PassThrough)
  throw new Error('This module requires Node 0.10 or above');

util.inherits(MantaCouch, EE);

function MantaCouch(opts) {
  if (!(this instanceof MantaCouch))
    return new MantaCouch(opts);

  EE.call(this);

  if (!opts || typeof opts !== 'object')
    throw new TypeError('opts object required');

  this.opts = opts;

  if (!opts.client || opts.client.constructor.name !== 'MantaClient')
    throw new TypeError('opts.client of type MantaClient is required');
  this.client = opts.client;

  if (opts.seqFile && typeof opts.seqFile !== 'string')
    throw new TypeError('opts.seqFile must be of type string');
  this.seqFile = null;
  if (opts.seqFile)
    this.seqFile = new SeqFile(opts.seqFile);

  if (!opts.path || typeof opts.path !== 'string')
    throw new TypeError('opts.path is required');
  this.path = opts.path
    .replace(/\/+$/, '')
    .replace(/^~~/, '/' + this.client.user);

  if (!opts.db || !url.parse(opts.db).protocol)
    throw new TypeError('opts.db url is required');
  this.db = opts.db.replace(/\/+$/, '');

  this.http = url.parse(this.db).protocol === 'https:' ?
    require('https') : require('http');

  if (opts.inactivity_ms && typeof opts.inactivity_ms !== 'number')
    throw new TypeError('opts.inactivity_ms must be of type number');
  this.inactivity_ms = opts.inactivity_ms;

  if (opts.seq && typeof opts.seq !== 'number')
    throw new TypeError('opts.seq must be of type number');
  this.seq = opts.seq || 0;
  if (this.seqFile)
    this.seqFile.seq = this.seq;

  if (opts.concurrency && typeof opts.concurrency !== 'number')
    throw new TypeError('opts.concurrency must be of type number');
  this.concurrency = opts.concurrency;

  this.delete = !!opts.delete;
  this.forensic = !!opts.forensic;

  this.started = false;
  this.start();
}

MantaCouch.prototype.start = function() {
  if (this.started)
    throw new Error('Already started');

  this.started = true;

  if (this.seq || !this.seqFile)
    this.onReadSeq(null, this.seq);
  else
    this.seqFile.read(this.onReadSeq.bind(this));
}

MantaCouch.prototype.onReadSeq = function(er, data) {
  this.seq = data;
  this.follow = follow({
    db: this.db,
    since: this.seq,
    inactivity_ms: this.inactivity_ms
  }, this.onChange.bind(this));
}

MantaCouch.prototype.onChange = function(er, change) {
  if (er)
    return this.emit('error', er);

  this.seq = change.seq;
  if (this.seqFile)
    this.seqFile.seq = change.seq;

  // Please don't delete the entire store in Manta, kthx
  if (!change.id)
    return;

  this.emit('change', change);
  if (change.deleted)
    this.rm(change);
  else
    this.put(change);
}

MantaCouch.prototype.rm = function(change) {
  if (this.delete || this.forensic) {
    this.pause();
    var p = this.path + '/' + change.id
    var cb = this.onRm.bind(this, change);
    if (this.forensic)
      this.forensicRm(change, p, cb);
    else
      this.client.rmr(p, cb);
  }
}

// get the list of p/_delete-$n folders
// create a new p/_delete-${n+1} folder
// move _attachments and *.json into there
// write {_id:$id,_deleted:true,_rev:$rev} to doc.json
MantaCouch.prototype.forensicRm = function(change, p, cb) {
  var dirs = [];
  var objs = [];
  this.client.ls(p, function(er, res) {
    if (er)
      return this.emit('error', er);
    res.on('object', function(obj) {
      objs.push(obj.name);
    });
    res.on('directory', function(dir) {
      dirs.push(dir.name);
    });
    res.once('error', this.emit.bind(this, 'error'));
    res.on('end', function() {
      change.dirs = dirs;
      change.objs = objs;
      this._forensicRm2(change, p, cb);
    }.bind(this));
  }.bind(this));
}

MantaCouch.prototype._forensicRm2 = function(change, p, cb) {
  var atts = [];
  this.client.ls(p + '/_attachments', function(er, res) {
    if (er && er.statusCode == 404)
      resOnEnd.call(this);

    if (er)
      return this.emit('error', er);

    res.on('object', function(obj) {
      atts.push(obj.name);
    });

    res.once('error', this.emit.bind(this, 'error'));
    res.on('end', resOnEnd.bind(this));
  }.bind(this));

  function resOnEnd() {
    change.atts = atts;
    this._forensicRm3(change, p, cb);
  }
}

MantaCouch.prototype._forensicRm3 = function(change, p, cb) {
  var nextDel = change.dirs.filter(function(d) {
    return /^_deleted-([0-9]+)$/.test(d);
  }).map(function(d) {
    return +(d.match(/^_deleted-([0-9]+)$/)[1]);
  }).sort().pop();

  if (isNaN(nextDel))
    nextDel = 0
  else
    nextDel += 1

  var delDir = p + '/_deleted-' + nextDel;
  var delAtt = delDir + '/_attachments';

  this.client.mkdirp(delAtt, function(er) {
    if (er)
      return this.emit('error', er);

    // link each attachment into the delAtt,
    // and each json into the delDir
    n = change.objs.length + change.atts.length;
    if (n === 0) {
      n = 0;
      return then();
    }

    change.objs.forEach(function(f) {
      var s = p + '/' + f;
      var d = delDir + '/' + f;
      this.client.ln(s, d, then.bind(this));
    }.bind(this));

    change.atts.forEach(function(a) {
      var s = p + '/_attachments/' + a;
      var d = delAtt + '/' + a;
      this.client.ln(s, d, then.bind(this));
    }.bind(this));
  }.bind(this));

  var n = 0;
  var errState = null;
  function then(er) {
    if (errState)
      return
    if (er)
      return this.emit('error', errState = er);
    if (--n === 0)
      this._forensicRm4(change, p, cb);
  }
}

MantaCouch.prototype._forensicRm4 = function(change, p, cb) {
  // we've linked all the objs and attachments to the trash folder.
  // Now, delete them, and mput the tombstone over doc.json
  var files = change.objs.map(function(f) {
    return p + '/' + f;
  }).concat(change.atts.map(function(f) {
    return p + '/_attachments/' + f;
  }));
  files.forEach(function(f) {
    this.client.unlink(f, then.bind(this));
  }.bind(this));
  var n = files.length;
  var errState = null;
  function then(er) {
    if (errState)
      return;
    if (er)
      return this.emit('error', errState = er);
    if (--n === 0)
      this._forensicRm5(change, p, cb);
  }
}

MantaCouch.prototype._forensicRm5 = function(change, p, cb) {
  var s = new PassThrough();
  change.rev = change.changes[change.changes.length-1].rev;
  var doc = {
    _id: change.id,
    _rev: change.rev,
    _deleted: true
  };

  var body = new Buffer(JSON.stringify(doc));
  s.end(body);
  var o = { size: body.length };
  this.client.put(p + '/doc.json', s, o, cb);
}

MantaCouch.prototype.onRm = function(change, er) {
  if (!er)
    this.emit('rm', change);

  if (!er || er.statusCode === 404)
    this.resume();
  else
    this.emit('error', er);
}

MantaCouch.prototype.stop =
MantaCouch.prototype.close =
MantaCouch.prototype.destroy = function() {
  if (this.client)
    this.client.close();
  if (this.follow)
    this.follow.die()
}

MantaCouch.prototype.put = function(change) {
  if (change.id !== encodeURIComponent(change.id)) {
    console.error('WARNING: Skipping %j\nWARNING: See %s', change.id,
                 'https://github.com/joyent/node-manta/issues/157')
    return
  }

  this.pause();
  var query = 'att_encoding_info=true&revs=true';
  var u = url.parse(this.db + '/' + change.id + '?' + query);
  this.http.get(u, parse(function(er, doc, res) {
    // If the doc was deleted, then just move on.
    // The removal will show up later in the _changes feed.
    if (er && er.statusCode === 404)
      return this.resume();

    if (er)
      return this.emit('error', er);

    change.doc = doc;
    change.rev = doc._rev;

    if (!doc._attachments)
      doc._attachments = {};

    this._put(change);
  }.bind(this)))
}

MantaCouch.prototype._put = function(change) {
  var doc = change.doc;

  var files = Object.keys(doc._attachments || {}).reduce(function (s, k) {
    var att = doc._attachments[k];
    // Gzip-encoded attachments are lying liars playing lyres
    if (att.encoding === 'gzip') {
      att.gzip_digest = att.digest;
      att.gzip_length = att.length;
      delete att.digest;
      delete att.length;
    }
    s['_attachments/' + k] = doc._attachments[k];
    return s;
  }, {});

  var json = new Buffer(JSON.stringify(doc) + '\n', 'utf8');
  files['doc.json'] = {
    type: 'application/json',
    name: 'doc.json',
    length: json.length
  };

  if (this.forensic)
    cb = this.forensicComplete.bind(this, change)
  else
    cb = this.onCuttleComplete.bind(this, change)

  cuttlefish({
    path: this.path + '/' + doc._id,
    client: this.client,
    files: files,
    getMd5: this.getMd5.bind(this, change, json),
    request: this.getFile.bind(this, change, json),
    delete: this.delete && !this.forensic,
    concurrency: this.concurrency
  })
    .on('send', this.emit.bind(this, 'send', change))
    .on('delete', this.emit.bind(this, 'delete', change))
    .on('error', this.emit.bind(this, 'error'))
    .on('complete', cb);
}

MantaCouch.prototype.getMd5 = function(change, json, file, cb) {
  if (file.name === 'doc.json')
    var md5 = crypto.createHash('md5').update(json).digest('base64');
  cb(null, md5);
}

MantaCouch.prototype.getFile = function(change, json, file, cb) {
  if (file.name === 'doc.json')
    this.streamDoc(json, file, cb)
  else
    this.getAttachment(change, file, cb)
}

MantaCouch.prototype.streamDoc = function(json, file, cb) {
  var s = new PassThrough();
  s.end(json);
  cb(null, s);
}

MantaCouch.prototype.getAttachment = function(change, file, cb) {
  var a = path.dirname(file.name).replace(/^_attachments/, change.id);
  var f = encodeURIComponent(path.basename(file.name))
  a += '/' + f
  this.emit('attachment', change, file);
  var u = this.db + '/' + a;
  this.http.get(u, function(res) {
    cb(null, res);
  }).on('error', cb);
}

MantaCouch.prototype.onCuttleComplete = function(change, results) {
  this.emit('put', change);
  this.emit('complete', change, results);
  this.resume();
}

MantaCouch.prototype.forensicComplete = function(change, results) {
  var links = {}
  links['doc.json'] = change.rev + '.json';
  Object.keys(change.doc._attachments).forEach(function(f) {
    var a = change.doc._attachments[f];
    f = '_attachments/' + f;

    var digest = a.digest || a.gzip_digest;
    if (a.skip || !digest)
      return;

    // hex is better for filenames.  No punctuation.
    var d = digest.replace(/^md5-/, '');
    d = new Buffer(d, 'base64').toString('hex');
    links[f] = f + '-' + d;
  });

  var keys = Object.keys(links);
  var n = keys.length;
  var errState = null;
  var p = this.path + '/' + change.id;
  keys.forEach(function(k) {
    var s = p + '/' + k;
    var d = p + '/' + links[k];
    this.client.ln(s, d, then.bind(this));
  }.bind(this));

  function then(er) {
    if (errState)
      return
    if (er)
      return this.emit('error', er);
    if (--n === 0)
      this.onCuttleComplete(change, results);
  }
}

MantaCouch.prototype.pause = function() {
  this.follow.pause();
};

MantaCouch.prototype.resume = function() {
  if (this.seqFile)
    this.seqFile.save();
  this.follow.resume();
}
