'use strict';

var implementation = require('./implementation');

module.exports = function getPolyfill() {
	return typeof Iterator === 'function' && typeof Iterator.prototype.includes === 'function'
		? Iterator.prototype.includes
		: implementation;
};
