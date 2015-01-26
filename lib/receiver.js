var ReadableStream = require('stream').Readable;
var redisEmitter = require('redis-eventemitter');
var util = require('util');
var asap = require('asap');
var uuid = require('node-uuid');
var debug = require('debug')('interprocess-pull-stream:receiver');

util.inherits(InterprocessReceiver, ReadableStream);

function InterprocessReceiver(options) {
  if (!(this instanceof InterprocessReceiver))
    return new InterprocessReceiver(options);

  ReadableStream.call(this, {
    objectMode: true
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

  this._nodeMeta = {
    id: uuid.v4()
  };

  this._init = false;
  this._retryInterval;
  this._retryTimeout = options.timeout || 10000;
  this._source = source;
  this._dataEvent = options.channel + ':data:' + this._nodeMeta.id;
  this._readyEvent = options.channel + ':ready';

  this._source.on(this._dataEvent, function(channel, data) {
    debug('channel:', channel, 'data:', data);
    this._clearTimeout();
    asap(self.push.bind(this, data));
  }.bind(this));
}

InterprocessReceiver.prototype._clearTimeout = function() {
  clearInterval(this._retryInterval);
  this._retryInterval = null;
};

InterprocessReceiver.prototype._ready = function() {
  asap(this._source.emit.bind(
    this,
    this._readyEvent,
    this._nodeMeta
  ));

  this._init = true;
};

InterprocessReceiver.prototype._read = function() {
  if (this._init) this._ready();

  if (!this._retryInterval) {
    this._retryInterval = setInterval(
      this._ready.bind(this),
      this._retryTimeout
    );
  }
};

module.exports = InterprocessReceiver;
