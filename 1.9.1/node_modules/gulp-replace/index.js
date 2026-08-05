'use strict';

const Transform = require('stream').Transform;
const rs = require('replacestream');
const istextorbinary = require('istextorbinary');

const defaultOptions = {
  skipBinary: true
}

module.exports = function(search, _replacement, options = {}) {
  // merge options
  options = {
    ...defaultOptions,
    ...options
  }

  return new Transform({
    objectMode: true,
    /**
     * transformation
     * @param {import("vinyl")} file 
     * @param {BufferEncoding} enc 
     * @param {(error?: Error | null, data?: any) => void} callback 
     */
    transform(file, enc, callback) {
      if (file.isNull()) {
        return callback(null, file);
      }

      let replacement = _replacement;
      if (typeof _replacement === 'function') {
        // Pass the vinyl file object as this.file
        replacement = _replacement.bind({ file: file });
      }

      function doReplace() {
        if (file.isStream()) {
          file.contents = file.contents.pipe(rs(search, replacement));
          return callback(null, file);
        }

        if (file.isBuffer()) {
          if (search instanceof RegExp) {
            file.contents =  Buffer.from(String(file.contents).replace(search, replacement));
          } else {
            const chunks = String(file.contents).split(search);

            let result;
            if (typeof replacement === 'function') {
              // Start with the first chunk already in the result
              // Replacements will be added thereafter
              // This is done to avoid checking the value of i in the loop
              result = [ chunks[0] ];

              // The replacement function should be called once for each match
              for (let i = 1; i < chunks.length; i++) {
                // Add the replacement value
                result.push(replacement(search));

                // Add the next chunk
                result.push(chunks[i]);
              }

              result = result.join('');
            }
            else {
              result = chunks.join(replacement);
            }

            file.contents =  Buffer.from(result);
          }
          return callback(null, file);
        }

        callback(null, file);
      }

      if (options.skipBinary) {
        if (!istextorbinary.isText(file.path, file.contents)) {
          callback(null, file);
        } else {
          doReplace();
        }
        return;
      }

      doReplace();
    }
  });
};
