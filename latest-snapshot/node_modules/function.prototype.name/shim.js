'use strict';

var supportsDescriptors = require('has-property-descriptors')();
var functionsHaveNames = require('functions-have-names')();
var $TypeError = require('es-errors/type');

var getPolyfill = require('./polyfill');
var defineProperty = require('es-define-property');

/** @type {import('./shim')} */
module.exports = function shimName() {
	var polyfill = getPolyfill();
	if (functionsHaveNames) {
		return polyfill;
	}
	if (!supportsDescriptors || !defineProperty) {
		throw new $TypeError('Shimming Function.prototype.name support requires ES5 property descriptor support.');
	}

	var functionProto = Function.prototype;
	defineProperty(functionProto, 'name', {
		configurable: true,
		enumerable: false,
		get: function () {
			var name = polyfill.call(this);
			if (this !== functionProto) {
				/** @type {Exclude<typeof defineProperty, false>} */ (defineProperty)(this, 'name', {
					configurable: true,
					enumerable: false,
					value: name,
					writable: false
				});
			}
			return name;
		}
	});
	return polyfill;
};
