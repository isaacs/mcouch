module.exports = MantaCouch;

var follow = require('follow');
var url = require('url');
var crypto = require('crypto');
var assert = require('assert');

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
  this.seqFile = opts.seqFile || null;
  if (this.seqFile)
    this.seqFile = path.resolve(this.seqFile);

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

  if (opts.concurrency && typeof opts.concurrency !== 'number')
    throw new TypeError('opts.concurrency must be of type number');
  this.concurrency = opts.concurrency;

  this.delete = !!opts.delete;

  this.following = false;
  this.savingSeq = false;
  this.start();
}

MantaCouch.prototype.saveSeq = function(file) {
  file = file || this.seqFile;
  if (!file && !this.seqFile)
    return
  if (!file)
    throw new Error('invalid sequence file: ' + file);
  if (!this.savingSeq)
    fs.writeFile(file, this.seq + '\n', 'ascii', this.afterSave.bind(this));
  this.savingSeq = true;
}

MantaCouch.prototype.start = function() {
  if (this.following)
    throw new Error('Cannot read sequence after follow starts');
  if (!this.seqFile) {
    this.seq = 0;
    this.onReadSeq();
  } else
    fs.readFile(this.seqFile, 'ascii', this.onReadSeq.bind(this));
}

MantaCouch.prototype.onReadSeq = function(er, data) {
  if (er && er.code === 'ENOENT')
    data = 0;
  else if (er)
    return this.emit('error', er);

  if (data === undefined)
    data = null
  if (!+data && +data !== 0)
    return this.emit('error', new Error('invalid data in seqFile'));

  data = +data;
  this.seq = +data;
  this.follow = follow({
    db: this.db,
    since: this.seq,
    inactivity_ms: this.inactivity_ms
  }, this.onChange.bind(this));
  this.following = true;
}

MantaCouch.prototype.afterSave = function(er) {
  if (er)
    this.emit('error', er);
  this.savingSeq = false;
}

MantaCouch.prototype.onChange = function(er, change) {
  if (er)
    return this.emit('error', er);

  this.seq = change.seq;

  // Please don't delete the entire store in Manta, kthx
  if (!change.id)
    return;

  if (change.deleted)
    this.rm(change);
  else
    this.put(change);
}

MantaCouch.prototype.rm = function(change) {
  if (this.delete) {
    this.emit('rm', change);
    this.pause();
    this.client.rmr(this.path + '/' + change.id, this.onRm.bind(this, change));
  }
}

MantaCouch.prototype.onRm = function(change, er) {
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
    this.follow.stop();
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
    if (er)
      return this.emit('error', er);
    change.doc = doc;
    this._put(change);
  }.bind(this)))
}

MantaCouch.prototype._put = function(change) {
  this.emit('put', change);
  var doc = change.doc;

  var files = Object.keys(doc._attachments || {}).reduce(function (s, k) {
    var att = doc._attachments[k];
    // Gzip-encoded attachments are lying liars playing lyres
    if (att.encoding === 'gzip') {
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

  cuttlefish({
    path: this.path + '/' + doc._id,
    client: this.client,
    files: files,
    getMd5: this.getMd5.bind(this, change, json),
    request: this.getFile.bind(this, change, json),
    delete: this.delete,
    concurrency: this.concurrency
  })
    .on('send', this.emit.bind(this, 'send', change))
    .on('delete', this.emit.bind(this, 'delete', change))
    .on('error', this.emit.bind(this, 'error'))
    .on('complete', this.onCuttleComplete.bind(this, change));
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
  this.emit('complete', change, results);
  this.resume();
};

MantaCouch.prototype.pause = function() {
  this.follow.pause();
};

MantaCouch.prototype.resume = function() {
  this.saveSeq();
  this.follow.resume();
}
