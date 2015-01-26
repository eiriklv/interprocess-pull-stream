var WritableStream = require('stream').Writable;
var redisEmitter = require('redis-eventemitter');
var util = require('util');
var asap = require('asap');
var debug = require('debug')('interprocess-pull-stream:transmitter');

util.inherits(InterprocessTransmitter, WritableStream);

function InterprocessTransmitter(options) {
  if (!(this instanceof InterprocessTransmitter))
    return new InterprocessTransmitter(options);

  WritableStream.call(this, {
    objectMode: true,
    highWaterMark: 1024
  });

  var parsedUrl = url.parse(options.url || '');

  var source = redisEmitter({
    port: parsedUrl.port || 6379,
    host: parsedUrl.hostname || '127.0.0.1',
    prefix: options.prefix,
    auth_pass: (parsedUrl.auth ? parsedUrl.auth.split(":")[1] : null),
    pub: options.pub,
    sub: options.sub
  });

  this._source = source;
  this._dataEvent = options.channel + ':data';
  this._readyEvent = options.channel + ':ready';
  this._readyTimeout = options.timeout ? (options.timeout + 1000) : 11000;
  this._ready = false;
  this._queue = [];
  this._nodes = [];

  this._source.on(this._readyEvent, function(channel, data) {
    this._addNode(data.id);

    if (this._queue.length > 0) {
      this._execute();
    } else {
      this._ready = true;
      asap(this.emit.bind(this, 'ready'));
    }
  }.bind(this));
}

InterprocessTransmitter.prototype._addNode = function(id) {
  var existing = this._nodes.filter(function(node) {
    return node.id === id;
  });

  if (existing.length > 0) {
    existing[0].time = Date.now();
  } else {
    this._nodes.push({
      id: id,
      time: Date.now()
    });
  }
};

InterprocessTransmitter.prototype._getNode = function() {
  var self = this;

  return (function getWorker() {
    if (self._nodes.length > 0) {
      var candidate = self._nodes.shift();

      if (!((Date.now() - candidate.time) < self._readyTimeout))
        return getWorker();
      return candidate;
    }
  }());
}

InterprocessTransmitter.prototype._execute = function() {
  var transmitMessage = this._queue.shift();
  var node = this._getNode();

  if (node) {
    transmitMessage(node.id);
  } else {
    this._queue.unshift(transmitMessage);
  }

  this._ready = this._nodes.length !== 0;
};

InterprocessTransmitter.prototype._transmit = function(chunk, done, nodeId) {
  var eventName = this._dataEvent + ':' + nodeId;

  asap(function() {
    this._source.emit(eventName, chunk);
    done();
  }.bind(this));
};

InterprocessTransmitter.prototype._write = function(chunk, encoding, done) {
  this._queue.push(this._transmit.bind(this, chunk, done));
  if (this._ready) this._execute();
};

module.exports = InterprocessTransmitter;
