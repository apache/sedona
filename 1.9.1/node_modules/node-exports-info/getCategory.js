'use strict';

var $RangeError = require('es-errors/range');
var entries = require('object.entries');
var satisfies = require('semver').satisfies;

var ranges = require('./ranges');

/** @type {import('./getCategory')} */
module.exports = function getCategory() {
	var version = arguments.length > 0 ? arguments[0] : process.version;
	var rangeEntries = entries(ranges);
	for (var i = 0; i < rangeEntries.length; i += 1) {
		var entry = rangeEntries[i];
		if (satisfies(version, entry[0])) {
			return entry[1];
		}
	}

	throw new $RangeError('no category found for version ' + version);
};
