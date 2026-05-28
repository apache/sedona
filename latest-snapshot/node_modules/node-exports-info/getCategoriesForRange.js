'use strict';

var flatMap = require('array.prototype.flatmap');
var entries = require('object.entries');
var intersects = require('semver').intersects;

var ranges = require('./ranges');

/** @type {import('./getCategoriesForRange')} */
module.exports = function getCategoriesForRange(rangeA) {
	return flatMap(
		entries(ranges),
		/** @type {(entry: import('./types').RangePair) => import('./types').Category[] | []} */
		function (entry) {
			var rangeB = entry[0];
			var category = entry[1];
			return intersects(rangeA, rangeB) ? [category] : [];
		}
	);
};
