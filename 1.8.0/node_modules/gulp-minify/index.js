'use strict';
const through = require("through2");
const uglify = require('terser');
const minimatch = require('minimatch');
const path = require('path');
const Vinyl = require('vinyl');
const PluginError = require('plugin-error');
const colors = require('ansi-colors');
const reSourceMapComment = /\n\/\/# sourceMappingURL=.+?$/;
const pathSeparatorRe = /[\/\\]/g;
const jsExtensions = /\.m?js$/;

function parseExt(ext, byDefault = ".js") {

  let _ext = {};

  if (!ext) {
    _ext = {
      min: "-min" + byDefault,
      src: byDefault
    }
  } else if (typeof ext === "string") {
    _ext = {
      min: ext,
      src: byDefault
    }
  } else {
    _ext = {
      min: ext.min || "-min" + byDefault,
      src: ext.src || byDefault
    }
  }

  return _ext;

}

function formatError(error, file) {
  let filePath = error.file === 'stdin' ? file.path : error.file;
  let message = '';

  filePath = filePath ? filePath : file.path;
  let relativePath = path.relative(process.cwd(), filePath);

  message += colors.underline(relativePath) + '\n';
  message += error.message + ' (line: ' + error.line  + ', col: ' + error.col + ', pos: ' + error.pos;
  error.message = colors.red(message);
  return error;
}

module.exports = function(opt) {

  //Set the options to the one provided or an empty object.
  let options = opt || {};

  //Set options output to itself, or, if null an empty object.
  options.output =  options.output ||  {};

  function minify(file, encoding, callback) {

    if (file.isNull()) {
      this.push(file);
      return callback();
    }

    if (file.isStream()) {
      this.emit('end');
      return new callback(PluginError('gulp-minify', 'Streaming not supported:' + file.path));
    }

    let ignore = false;

    if (options.exclude) {
      ignore = options.exclude.some(function(item) {
        return path.dirname(file.path).split(pathSeparatorRe).some(function(pathName) {
          return minimatch(pathName, item);
        });
      });
    }

    if (!ignore && options.ignoreFiles) {
      ignore = options.ignoreFiles.some(function(item) {
        return minimatch(path.basename(file.path), item);
      });
    }

    if (ignore || !path.extname(file.path).match(jsExtensions)) {
      this.push(file);
      return callback();
    }

    let mangled, originalSourceMap;

    if (file.sourceMap) {
      options.outSourceMap = file.relative;
      if (file.sourceMap.mappings !== '') {
        options.inSourceMap = file.sourceMap;
      }
      originalSourceMap = file.sourceMap;
    }

    if (options.preserveComments === 'all') {
      options.output.comments = true;
    } else if (options.preserveComments === 'some') {

      options.output.comments = /^!|@preserve|@license|@cc_on/i;
    } else if (typeof options.preserveComments === 'function') {
      options.output.comments = options.preserveComments;
    }
    options.fromString = options.hasOwnProperty("fromString") ? options.fromString : true;

    //Parse the extensions form the options.
    let ext = parseExt(options.ext, path.extname(file.path));

    let min_file = new Vinyl({
      base: file.base,
      path: Array.isArray(ext.min) ? file.path.replace(ext.min[0], ext.min[1]) : file.path.replace(jsExtensions, ext.min),
    });

    const uglifyOptions = {
      mangle   : options.mangle   !== undefined ? options.mangle : true,
      output   : options.output   !== undefined ? options.output : null,
      compress : options.compress !== undefined ? options.compress : {},
      sourceMap: !!file.sourceMap
    };

    try {
      mangled = uglify.minify(String(file.contents), uglifyOptions);
      min_file.contents = new Buffer(mangled.code.replace(reSourceMapComment, ''));
    } catch (e) {
      this.emit('end');
      return callback(new PluginError('gulp-minify', formatError(e, file)));
    }

    if (file.sourceMap) {
      min_file.sourceMap = JSON.parse(mangled.map);
      min_file.sourceMap.sourcesContent = originalSourceMap.sourcesContent;
      min_file.sourceMap.sources = originalSourceMap.sources;
    }

    this.push(min_file);

    if (options.noSource !== true) {
      file.path = file.path.replace(jsExtensions, ext.src);
      this.push(file);
    }

    callback();
  }

  return through.obj(minify);
};
