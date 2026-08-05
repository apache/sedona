'use strict';

var $RangeError = require('es-errors/range');

var isCategory = require('./isCategory');

// Categories that support patterns (wildcard *)
/** @type {{ [k in import('./types').Category | '__proto__']?: k extends '__proto__' ? null : true }} */
var patternsCategories = {
	__proto__: null,
	patterns: true,
	'pattern-trailers': true,
	'pattern-trailers+json-imports': true,
	'pattern-trailers-no-dir-slash': true,
	'pattern-trailers-no-dir-slash+json-imports': true,
	'require-esm': true,
	'strips-types': true,
	'subpath-imports-slash': true
};

// Categories that support pattern trailers (suffix after *)
/** @type {{ [k in import('./types').Category | '__proto__']?: k extends '__proto__' ? null : true }} */
var patternTrailersCategories = {
	__proto__: null,
	'pattern-trailers': true,
	'pattern-trailers+json-imports': true,
	'pattern-trailers-no-dir-slash': true,
	'pattern-trailers-no-dir-slash+json-imports': true,
	'require-esm': true,
	'strips-types': true,
	'subpath-imports-slash': true
};

// Categories that support directory slash exports (ending with /)
/** @type {{ [k in import('./types').Category | '__proto__']?: k extends '__proto__' ? null : true }} */
var dirSlashCategories = {
	__proto__: null,
	'broken-dir-slash-conditions': true,
	patterns: true,
	'pattern-trailers': true,
	'pattern-trailers+json-imports': true,
	'subpath-imports-slash': true
};

/** @type {import('./getCategoryFlags')} */
module.exports = function getCategoryFlags(category) {
	if (!isCategory(category)) {
		throw new $RangeError('invalid category ' + category);
	}

	return {
		patterns: !!patternsCategories[category],
		patternTrailers: !!patternTrailersCategories[category],
		dirSlash: !!dirSlashCategories[category]
	};
};
