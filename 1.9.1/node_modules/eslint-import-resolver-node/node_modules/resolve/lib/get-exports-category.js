'use strict';

var isCategory = require('node-exports-info/isCategory');
var getCategoriesForRange = require('node-exports-info/getCategoriesForRange');
var $TypeError = require('es-errors/type');

var selectMostRestrictive = require('./select-most-restrictive');

// Determine the active exports category from resolve options
// Returns null if no exports resolution should be applied
// Returns 'engines' if engines: true (needs consumer package.json lookup)
// Throws TypeError if invalid options are provided
/** @type {(opts?: { exportsCategory?: import('node-exports-info/getCategory').Category, engines?: boolean | string }) => null | import('node-exports-info/getCategory').Category} */
module.exports = function getExportsCategory(opts) {
    if (!opts) {
        return null;
    }

    var hasCategory = typeof opts.exportsCategory !== 'undefined';
    var engines = opts.engines;
    var hasEngines = typeof engines !== 'undefined' && engines !== false;

    if (hasCategory && hasEngines) {
        throw new $TypeError('`exportsCategory` and `engines` are mutually exclusive.');
    }

    if (hasCategory) {
        if (!isCategory(opts.exportsCategory)) {
            var catError = new $TypeError('Invalid exports category: "' + opts.exportsCategory + '"');
            catError.code = 'INVALID_EXPORTS_CATEGORY';
            throw catError;
        }
        return opts.exportsCategory;
    }

    if (hasEngines) {
        // engines: true means read from consumer's package.json
        if (engines === true) {
            return 'engines';
        }

        // engines must be a non-empty string (semver range)
        if (typeof engines !== 'string' || engines === '') {
            throw new $TypeError('`engines` must be `true`, `false`, or a non-empty string semver range.');
        }

        var categories = getCategoriesForRange(engines);
        return selectMostRestrictive(categories);
    }

    return null;
};
