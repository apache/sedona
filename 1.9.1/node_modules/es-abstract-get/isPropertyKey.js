'use strict';

/** @type {import('./isPropertyKey')} */
module.exports = function isPropertyKey(argument) {
	return typeof argument === 'string' || typeof argument === 'symbol';
};
