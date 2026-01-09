var fancyLog = require('fancy-log');
var colors = require('ansi-colors');
var notifier = require("node-notifier");
var report = require("./report");

"use strict";

// Default log level
var logLevel = 2;

// Default logger
var fnLog = fancyLog;

var logError = module.exports.logError = function (options, isError) {
  if (!logLevel) return;
  if (logLevel === 1 && !isError) return;

  var color = isError ? "red" : "green";
  if (!colors[color]) return;
  fnLog(colors.cyan('gulp-notify') + ':',
           '[' + colors.blue(options.title) + ']',
            colors[color](options.message)
           );
};


// Expose onError behaviour
module.exports.onError = function (options, callback) {
  var reporter;
  options = options || {};
  var templateOptions = options.templateOptions || {};
  callback = callback || function (err) {
    err && logError({
      title: "Error running notifier",
      message: "Could not send message: " + err.message
    }, true);
  };

  if (options.notifier) {
    reporter = options.notifier;
  } else {
    if (options.host || options.appName || options.port) {
      notifier = new notifier.Notification({
        host: options.host || 'localhost',
        appName: options.appName || 'gulp-notify',
        port: options.port || '23053'
      });
    }
    reporter = notifier.notify.bind(notifier);
  }
  return function (error) {
    var self = this;
    report(reporter, error, options, templateOptions, function () {
      callback.apply(self, arguments);
      self.emit && self.emit('end');
    });
  };
};

// Expose to set log level
module.exports.logLevel = function (newLogLevel) {
  if (newLogLevel === void 0) return logLevel;
  logLevel = newLogLevel;
};

// Expose to set new logger
module.exports.logger = function (newLogger) {
  if (!newLogger) return fnLog;
  fnLog = newLogger;
};
