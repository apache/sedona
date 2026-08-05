'use strict';

var GetIntrinsic = require('get-intrinsic');
var hasPropertyDescriptors = require('has-property-descriptors')();

var define = require('define-properties');
var gOPD = require('gopd');

var SetterThatIgnoresPrototypeProperties = require('es-abstract/2025/SetterThatIgnoresPrototypeProperties');

var getPolyfill = require('./polyfill');

var $IteratorPrototype = require('../Iterator.prototype/implementation');

var $defineProperty = hasPropertyDescriptors && GetIntrinsic('%Object.defineProperty%', true);

module.exports = function shimIteratorPrototypeCtor() {
	var polyfill = getPolyfill();

	if ($defineProperty) {
		var desc = gOPD($IteratorPrototype, 'constructor');
		if (!desc || !desc.get || desc.get.call() !== polyfill) {
			$defineProperty($IteratorPrototype, 'constructor', {
				configurable: true,
				enumerable: false,
				get: function () {
					return polyfill; // step 1
				},
				set: function (v) {
					SetterThatIgnoresPrototypeProperties(this, $IteratorPrototype, 'constructor', v); // step 1
				}
			});
		}
	} else {
		define(
			$IteratorPrototype,
			{ constructor: polyfill },
			{ constructor: function () { return $IteratorPrototype.constructor !== polyfill; } }
		);
	}

	return polyfill;
};
