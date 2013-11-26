module.exports = MantaCouch;

var follow = require('follow');
var url = require('url');
var crypto = require('crypto');

var cuttlefish = require('cuttlefish');

var path = require('path');
var fs = require('fs');
var PassThrough = require('stream').PassThrough;

if (!PassThrough)
  throw new Error('This module requires Node 0.10 or above');

function MantaCouch(opts) {
  if (!(this instanceof MantaCouch))
    return new MantaCouch(opts);

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
  this.path = opts.path.replace(/\/+$/, '');

  if (!opts.db || !url.parse(opts.db).protocol)
    throw new TypeError('opts.db url is required');
  this.db = opts.db.replace(/\/+$/, '');

  this.http = url.parse(this.db).prototcol === 'https:' ?
    require('https') : require('http');

  if (opts.inactivity_ms && typeof opts.inactivity_ms !== 'number')
    throw new TypeError('opts.inactivity_ms must be of type number');
  this.inactivity_ms = opts.inactivity_ms;

  if (opts.seq && typeof opts.seq !== 'number')
    throw new TypeError('opts.seq must be of type number');
  this.seq = opts.seq || 0;

  this.delete = !!opts.delete;

  this.following = false;
  this.savingSeq = false;
  this.start();
}

MantaCouch.prototype.saveSeq = function(file) {
  file = file || this.seqFile;
  if (!file)
    throw new Error('invalid sequence file: ' + file);
  if (!this.savingSeq)
    fs.writeFile(file, this.seq + '\n', 'ascii', this.afterSave.bind(this));
  this.savingSeq = true;
}

MantaCouch.prototype.readSeq = function(cb) {
  if (this.following)
    throw new Error('Cannot read sequence after follow starts');
  if (!this.seqFile) {
    this.seq = 0;
    cb();
  } else
    fs.readFile(this.seqFile, 'ascii', this.onReadSeq.bind(this));
}

MantaCouch.prototype.onReadSeq = function(er, data) {
  if (er)
    throw er;
  data = +data;
  if (!data && data !== 0)
    throw new Error('invalid data in seqFile');
  this.seq = +data;
  this.follow();
}

MantaCouch.prototype.afterSave = function(er) {
  if (er)
    throw er;
  this.savingSeq = false;
}

MantaCouch.prototype.start = function() {
  this.readSeq(this.follow.bind(this));
};

MantaCouch.prototype.follow = function() {
  this.follow = follow({
    db: this.db,
    since: this.seq,
    include_docs: true,
    inactivity_ms: this.inactivity_ms
  }, this.onChange.bind(this));
}

MantaCouch.prototype.onChange = function(er, change) {
  if (er)
    throw er;
  else if (change.deleted)
    this.rm(change);
  else
    this.put(change);
}

MantaCouch.prototype.rm = function(change) {
  if (this.delete) {
    this.log('RM %s', change.id);
    this.follow.pause();
    this.client.rmr(this.path + '/' + change.id, this.onDelete.bind(this));
  }
}

MantaCouch.prototype.onDelete = function(er) {
  if (!er || er.statusCode === 404)
    this.follow.resume();
  else
    throw er;
}

MantaCouch.prototype.put = function(change) {
  this.log('PUT %s', change.id);
  var doc = change.doc;

  var files = Object.keys(doc._attachments || {}).reduce(function (s, k) {
    s['_attachments/' + k] = doc._attachments[k];
    return s;
  }, {});

  var json = new Buffer(JSON.stringify(doc) + '\n', 'utf8');
  files['doc.json'] = {
    type: 'application/json',
    name: 'doc.json',
    length: json.length
  };

  doc._json = json;

  cuttlefish({
    path: this.path + '/' + change.id,
    client: this.client,
    files: files,
    getMd5: this.getMd5.bind(this, doc),
    request: this.getFile.bind(this, doc),
    delete: this.delete
  })
    .on('send', this.onCuttleSend.bind(this, doc))
    .on('delete', this.onCuttleDelete.bind(this, doc))
    .on('complete', this.onCuttleComplete.bind(this));
};

// XXX
MantaCouch.prototype.onCuttleSend = function(doc, file, response) {
  this.log('-> %d SENT %s/%s', response.statusCode, doc._id, file.name);
};

MantaCouch.prototype.onCuttleDelete = function(doc, file, response) {
  this.log('-> %d DELETED %s/%s', response.statusCode, doc._id, file.name);
};


MantaCouch.prototype.getMd5 = function(doc, file, cb) {
  assert.equal(file.name, 'doc.json');
  var md5 = crypto.createHash('md5').update(doc._json).digest('base64');
  cb(null, md5);
}

MantaCouch.prototype.getFile = function(doc, file, cb) {
  if (file.name === 'doc.json')
    this.streamDoc(doc, file, cb)
  else
    this.getAttachment(doc, file, cb)
}

MantaCouch.prototype.streamDoc = function(doc, file, cb) {
  this.log('streamDoc', doc, file, doc._json, cb);
  var s = new PassThrough();
  s.end(doc._json);
  cb(null, s);
}

MantaCouch.prototype.getAttachment = function(doc, file, cb) {
  var a = file.name.replace(/^_attachments/, doc._id);
  this.log('PUT', a);
  var u = this.db + '/' + a;
  this.http.get(u, function(res) {
    cb(null, res);
  }).on('error', cb);
}

MantaCouch.prototype.onCuttleComplete = function(results) {
  this.log(results);
  this.follow.resume();
};


MantaCouch.prototype.log = function() {
  console.log.apply(console, arguments);
}
