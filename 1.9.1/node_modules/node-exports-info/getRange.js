'use strict';

var $RangeError = require('es-errors/range');
var entries = require('object.entries');

var ranges = require('./ranges');

/** @type {import('./getRange')} */
module.exports = function getRange(category) {
	var rangeEntries = entries(ranges);
	for (var i = 0; i < rangeEntries.length; i += 1) {
		var entry = rangeEntries[i];
		if (entry[1] === category) {
			return entry[0];
		}
	}

	throw new $RangeError('no version range found for category ' + category);
};
