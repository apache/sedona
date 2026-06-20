'use strict';

var implementation = require('./implementation');

/** @type {import('./polyfill')} */
module.exports = function getPolyfill() {
	return implementation;
};
