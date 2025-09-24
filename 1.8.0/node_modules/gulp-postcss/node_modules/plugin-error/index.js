var util = require('util');
var colors = require('ansi-colors');

var nonEnum = ['message', 'name', 'stack'];
var ignored = new Set(
  nonEnum.concat([
    '__safety',
    '_stack',
    'plugin',
    'showProperties',
    'showStack',
    'domain',
    'domainEmitter',
    'domainThrown',
  ])
);
var props = [
  'fileName',
  'lineNumber',
  'message',
  'name',
  'plugin',
  'showProperties',
  'showStack',
  'stack',
];

function PluginError(plugin, message, options) {
  if (!(this instanceof PluginError)) {
    return new PluginError(plugin, message, options);
  }

  Error.call(this);
  var opts = setDefaults(plugin, message, options);
  var self = this;

  // If opts has an error, get details from it
  if (typeof opts.error === 'object') {
    var keys = new Set(Object.keys(opts.error).concat(nonEnum));

    // These properties are not enumerable, so we have to add them explicitly.
    keys.forEach(function (prop) {
      self[prop] = opts.error[prop];
    });
  }

  // Opts object can override
  props.forEach(function (prop) {
    if (prop in opts) {
      this[prop] = opts[prop];
    }
  }, this);

  // Defaults
  if (!this.stack) {
    /**
     * `Error.captureStackTrace` appends a stack property which
     * relies on the toString method of the object it is applied to.
     *
     * Since we are using our own toString method which controls when
     * to display the stack trace, if we don't go through this safety
     * object we'll get stack overflow problems.
     */

    var safety = {};
    safety.toString = function () {
      return this._messageWithDetails() + '\nStack:';
    }.bind(this);

    Error.captureStackTrace(safety, arguments.callee || this.constructor);
    this.__safety = safety;
  }
  if (!this.plugin) {
    throw new Error('Missing plugin name');
  }
  if (!this.message) {
    throw new Error('Missing error message');
  }
}

util.inherits(PluginError, Error);

/**
 * Output a formatted message with details
 */

PluginError.prototype._messageWithDetails = function () {
  var msg = 'Message:\n    ' + this.message;
  var details = this._messageDetails();
  if (details !== '') {
    msg += '\n' + details;
  }
  return msg;
};

/**
 * Output actual message details
 */

PluginError.prototype._messageDetails = function () {
  if (!this.showProperties) {
    return '';
  }

  var props = Object.keys(this).filter(function (key) {
    return !ignored.has(key);
  });
  var len = props.length;

  if (len === 0) {
    return '';
  }

  var res = '';
  var i = 0;
  while (len--) {
    var prop = props[i++];
    res += '    ';
    res += prop + ': ' + this[prop];
    res += '\n';
  }
  return 'Details:\n' + res;
};

/**
 * Override the `toString` method
 */

PluginError.prototype.toString = function () {
  var detailsWithStack = function (stack) {
    return this._messageWithDetails() + '\nStack:\n' + stack;
  }.bind(this);

  var msg = '';
  if (this.showStack) {
    // If there is no wrapped error, use the stack captured in the PluginError ctor
    if (this.__safety) {
      msg = this.__safety.stack;
    } else if (this._stack) {
      msg = detailsWithStack(this._stack);
    } else {
      // Stack from wrapped error
      msg = detailsWithStack(this.stack);
    }
    return message(msg, this);
  }

  msg = this._messageWithDetails();
  return message(msg, this);
};

// Format the output message
function message(msg, thisArg) {
  var sig = colors.red(thisArg.name);
  sig += ' in plugin ';
  sig += '"' + colors.cyan(thisArg.plugin) + '"';
  sig += '\n';
  sig += msg;
  return sig;
}

/**
 * Set default options based on arguments.
 */

function setDefaults(plugin, message, opts) {
  if (typeof plugin === 'object') {
    return defaults(plugin);
  }
  if (message instanceof Error) {
    opts = Object.assign({}, opts, { error: message });
  } else if (typeof message === 'object') {
    opts = Object.assign({}, message);
  } else {
    opts = Object.assign({}, opts, { message: message });
  }
  opts.plugin = plugin;
  return defaults(opts);
}

/**
 * Extend default options with:
 *
 *  - `showStack`: default=false
 *  - `showProperties`: default=true
 *
 * @param  {Object} `opts` Options to extend
 * @return {Object}
 */

function defaults(opts) {
  return Object.assign(
    {
      showStack: false,
      showProperties: true,
    },
    opts
  );
}

/**
 * Expose `PluginError`
 */

module.exports = PluginError;
