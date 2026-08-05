'use strict';

var entries = require('object.entries');

var ranges = require('./ranges');

/** @type {import('./getRangePairs')} */
module.exports = function getRangePairs() {
	return entries(ranges);
};
