'use strict';

var define = require('define-properties');
var getPolyfill = require('./polyfill');

var $IteratorPrototype = require('../Iterator.prototype/implementation');

module.exports = function shimIteratorPrototypeIncludes() {
	var polyfill = getPolyfill();

	define(
		$IteratorPrototype,
		{ includes: polyfill },
		{ includes: function () { return $IteratorPrototype.includes !== polyfill; } }
	);

	return polyfill;
};
