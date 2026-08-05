'use strict';

var toStr = Object.prototype.toString;

var isPrimitive = require('./helpers/isPrimitive');

var isCallable = require('is-callable');
var $TypeError = require('es-errors/type');

/** @import { unknownES5 } from './es5' */

// http://ecma-international.org/ecma-262/5.1/#sec-8.12.8
var ES5internalSlots = /** @type {const} */ ({
	'[[DefaultValue]]': /** @param {object} O */ function (O) {
		var actualHint;
		if (arguments.length > 1) {
			actualHint = arguments[1];
		} else {
			actualHint = toStr.call(O) === '[object Date]' ? String : Number;
		}

		if (actualHint === String || actualHint === Number) {
			var methods = actualHint === String
				? /** @type {const} */ (['toString', 'valueOf'])
				: /** @type {const} */ (['valueOf', 'toString']);
			/** @type {unknownES5} */
			var value;
			for (var i = 0; i < methods.length; ++i) {
				var method = methods[i];
				if (isCallable(O[method])) {

					value = O[method]();
					if (isPrimitive(value)) {
						return value;
					}
				}
			}
			throw new $TypeError('No default value');
		}
		throw new $TypeError('invalid [[DefaultValue]] hint supplied');
	}
});

/** @type {import('./es5')} */
// http://ecma-international.org/ecma-262/5.1/#sec-9.1
module.exports = function ToPrimitive(input) {
	if (isPrimitive(input)) {
		return input;
	}
	if (arguments.length > 1) {

		return ES5internalSlots['[[DefaultValue]]'](input, arguments[1]);
	}

	return ES5internalSlots['[[DefaultValue]]'](input);
};
