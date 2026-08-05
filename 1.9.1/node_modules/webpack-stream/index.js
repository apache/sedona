'use strict';

const fancyLog = require('fancy-log');
const PluginError = require('plugin-error');
const supportsColor = require('supports-color');
const File = require('vinyl');
const MemoryFileSystem = require('memory-fs');
const nodePath = require('path');
const through = require('through');
const ProgressPlugin = require('webpack/lib/ProgressPlugin');
const clone = require('lodash.clone');

const defaultStatsOptions = {
  colors: supportsColor.stdout.hasBasic,
  hash: false,
  timings: false,
  chunks: false,
  chunkModules: false,
  modules: false,
  children: true,
  version: true,
  cached: false,
  cachedAssets: false,
  reasons: false,
  source: false,
  errorDetails: false
};

module.exports = function (options, wp, done) {
  const cache = {
    options: options,
    wp: wp
  };

  options = clone(options) || {};
  let config = options.config || options;

  const isInWatchMode = !!options.watch;
  delete options.watch;

  if (typeof config === 'string') {
    config = require(config);
  }

  // Webpack 4 doesn't support the `quiet` attribute, however supports
  // setting `stats` to a string within an array of configurations
  // (errors-only|minimal|none|normal|verbose) or an object with an absurd
  // amount of config
  const isSilent = options.quiet || (typeof options.stats === 'string' && (options.stats.match(/^(errors-only|minimal|none)$/)));

  if (typeof done !== 'function') {
    let callingDone = false;
    done = function (err, stats) {
      if (err) {
        // The err is here just to match the API but isnt used
        return;
      }
      stats = stats || {};
      if (isSilent || callingDone) {
        return;
      }

      // Debounce output a little for when in watch mode
      if (isInWatchMode) {
        callingDone = true;
        setTimeout(function () {
          callingDone = false;
        }, 500);
      }

      if (options.verbose) {
        fancyLog(stats.toString({
          colors: supportsColor.stdout.hasBasic
        }));
      } else {
        const statsOptions = (options && options.stats) || {};

        if (typeof statsOptions === 'object') {
          Object.keys(defaultStatsOptions).forEach(function (key) {
            if (typeof statsOptions[key] === 'undefined') {
              statsOptions[key] = defaultStatsOptions[key];
            }
          });
        }
        const statusLog = stats.toString(statsOptions);
        if (statusLog) {
          fancyLog(statusLog);
        }
      }
    };
  }

  const webpack = wp || require('webpack');
  let entry = [];
  const entries = Object.create(null);

  const stream = through(function (file) {
    if (file.isNull()) {
      return;
    }
    if ('named' in file) {
      if (!Array.isArray(entries[file.named])) {
        entries[file.named] = [];
      }
      entries[file.named].push(file.path);
    } else {
      entry = entry || [];
      entry.push(file.path);
    }
  }, function () {
    const self = this;
    const handleConfig = function (config) {
      config.output = config.output || {};

      // Determine pipe'd in entry
      if (Object.keys(entries).length > 0) {
        entry = entries;
        if (!config.output.filename) {
          // Better output default for multiple chunks
          config.output.filename = '[name].js';
        }
      } else if (entry.length < 2) {
        entry = entry[0] || entry;
      }

      config.entry = config.entry || entry;
      config.output.path = config.output.path || process.cwd();
      entry = [];

      if (!config.entry || config.entry.length < 1) {
        fancyLog('webpack-stream - No files given; aborting compilation');
        self.emit('end');
        return false;
      }
      return true;
    };

    let succeeded;
    if (Array.isArray(config)) {
      for (let i = 0; i < config.length; i++) {
        succeeded = handleConfig(config[i]);
        if (!succeeded) {
          return false;
        }
      }
    } else {
      succeeded = handleConfig(config);
      if (!succeeded) {
        return false;
      }
    }

    // Cache compiler for future use
    const compiler = cache.compiler || webpack(config);
    cache.compiler = compiler;

    const callback = function (err, stats) {
      if (err) {
        self.emit('error', new PluginError('webpack-stream', err));
        return;
      }
      const jsonStats = stats ? stats.toJson() || {} : {};
      const errors = jsonStats.errors || [];
      if (errors.length) {
        const resolveErrorMessage = (err) => {
          if (
            typeof err === 'object' &&
            err !== null &&
            Object.prototype.hasOwnProperty.call(err, 'message')
          ) {
            return err.message;
          } else if (
            typeof err === 'object' &&
            err !== null &&
            'toString' in err &&
            err.toString() !== '[object Object]'
          ) {
            return err.toString();
          } else if (Array.isArray(err)) {
            return err.map(resolveErrorMessage).join('\n');
          } else {
            return err;
          }
        };

        const errorMessage = errors.map(resolveErrorMessage).join('\n');
        const compilationError = new PluginError('webpack-stream', errorMessage);
        if (!isInWatchMode) {
          self.emit('error', compilationError);
        }
        self.emit('compilation-error', compilationError);
      }
      if (!isInWatchMode) {
        self.queue(null);
      }
      done(err, stats);
      if (isInWatchMode && !isSilent) {
        fancyLog('webpack is watching for changes');
      }
    };

    if (isInWatchMode) {
      const watchOptions = options.watchOptions || {};
      compiler.watch(watchOptions, callback);
    } else {
      compiler.run(callback);
    }

    const handleCompiler = function (compiler) {
      if (options.progress) {
        (new ProgressPlugin(function (percentage, msg) {
          percentage = Math.floor(percentage * 100);
          msg = percentage + '% ' + msg;
          if (percentage < 10) msg = ' ' + msg;
          fancyLog('webpack', msg);
        })).apply(compiler);
      }

      cache.mfs = cache.mfs || new MemoryFileSystem();

      const fs = compiler.outputFileSystem = cache.mfs;

      const assetEmittedPlugin = compiler.hooks
        // Webpack 4/5
        ? function (callback) { compiler.hooks.assetEmitted.tapAsync('WebpackStream', callback); }
        // Webpack 2/3
        : function (callback) { compiler.plugin('asset-emitted', callback); };

      assetEmittedPlugin(function (outname, _, callback) {
        const file = prepareFile(fs, compiler, outname);
        self.queue(file);
        callback();
      });
    };

    if (Array.isArray(options.config)) {
      compiler.compilers.forEach(function (compiler) {
        handleCompiler(compiler);
      });
    } else {
      handleCompiler(compiler);
    }

    if (options.watch && !isSilent) {
      const watchRunPlugin = compiler.hooks
        // Webpack 4/5
        ? callback => compiler.hooks.watchRun.tapAsync('WebpackInfo', callback)
        // Webpack 2/3
        : callback => compiler.plugin('watch-run', callback);

      watchRunPlugin((compilation, callback) => {
        fancyLog('webpack compilation starting...');
        callback();
      });
    }
  });

  // If entry point manually specified, trigger that
  const hasEntry = Array.isArray(config)
    ? config.some(function (c) { return c.entry; })
    : config.entry;
  if (hasEntry) {
    stream.end();
  }

  return stream;
};

function prepareFile (fs, compiler, outname) {
  let path = fs.join(compiler.outputPath, outname);
  if (path.indexOf('?') !== -1) {
    path = path.split('?')[0];
  }

  const contents = fs.readFileSync(path);

  const file = new File({
    base: compiler.outputPath,
    path: nodePath.join(compiler.outputPath, outname),
    contents: contents
  });
  return file;
}

// Expose webpack if asked
Object.defineProperty(module.exports, 'webpack', {
  get: function () {
    return require('webpack');
  }
});
