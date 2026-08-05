'use strict';

var GetIntrinsic = require('get-intrinsic');
var hasPropertyDescriptors = require('has-property-descriptors')();

var getPolyfill = require('./polyfill');
var define = require('define-properties');
var gOPD = require('gopd');
var hasSymbols = require('has-symbols')();
var setToStringTag = require('es-set-tostringtag');

var SetterThatIgnoresPrototypeProperties = require('es-abstract/2025/SetterThatIgnoresPrototypeProperties');

var getIteratorPolyfill = require('../Iterator/polyfill');

var $defineProperty = hasPropertyDescriptors && GetIntrinsic('%Object.defineProperty%', true);

module.exports = function shimIteratorFrom() {
	var $Iterator = getIteratorPolyfill();
	var polyfill = getPolyfill();
	define(
		$Iterator,
		{ prototype: polyfill },
		{ prototype: function () { return $Iterator.prototype !== polyfill; } }
	);

	if (hasSymbols && Symbol.toStringTag) {
		if ($defineProperty) {
			var desc = gOPD(polyfill, Symbol.toStringTag);
			if (!desc || !desc.get || desc.get.call() !== 'Iterator') {
				$defineProperty(polyfill, Symbol.toStringTag, {
					configurable: true,
					enumerable: false,
					get: function () {
						return 'Iterator'; // step 1
					},
					set: function (v) {
						SetterThatIgnoresPrototypeProperties(this, polyfill, Symbol.toStringTag, v); // step 1
					}
				});
			}
		} else {
			setToStringTag(polyfill, 'Iterator');
		}
	}

	return polyfill;
};
