'use strict';

var hasSymbols = typeof Symbol === 'function' && typeof Symbol.iterator === 'symbol';

var isCallable = require('is-callable');
var isDate = require('is-date-object');
var isSymbol = require('is-symbol');
var $TypeError = require('es-errors/type');

var isPrimitive = require('./helpers/isPrimitive');

/** @import { primitive } from './' */

/** @type {(O: { valueOf?: () => unknown, toString?: () => unknown }, hint: 'number' | 'string') => primitive} */
function OrdinaryToPrimitive(O, hint) {
	if (typeof O === 'undefined' || O === null) {
		throw new $TypeError('Cannot call method on ' + O);
	}
	if (typeof hint !== 'string' || (hint !== 'number' && hint !== 'string')) {
		throw new $TypeError('hint must be "string" or "number"');
	}
	/** @type {('toString' | 'valueOf')[]} */
	var methodNames = hint === 'string' ? ['toString', 'valueOf'] : ['valueOf', 'toString'];
	var method, result, i;
	for (i = 0; i < methodNames.length; ++i) {
		method = O[methodNames[i]];
		if (isCallable(method)) {
			result = method.call(O);
			if (isPrimitive(result)) {
				return result;
			}
		}
	}
	throw new $TypeError('No default value');
}

var GetMethod = require('es-abstract-get/GetMethod');

/** @type {import('./es2015')} */
// http://www.ecma-international.org/ecma-262/6.0/#sec-toprimitive
module.exports = function ToPrimitive(input) {
	if (isPrimitive(input)) {
		return input;
	}
	/** @type {'default' | 'string' | 'number'} */
	var hint = 'default';
	if (arguments.length > 1) {
		if (arguments[1] === String) {
			hint = 'string';
		} else if (arguments[1] === Number) {
			hint = 'number';
		}
	}

	var exoticToPrim;
	if (hasSymbols) {
		if (Symbol.toPrimitive) {

			exoticToPrim = GetMethod(
				/** @type {{ [k in SymbolConstructor['toPrimitive']]?: Function }} */
				(input),
				Symbol.toPrimitive
			);
		} else if (isSymbol(input)) {
			exoticToPrim = Symbol.prototype.valueOf;
		}
	}
	if (typeof exoticToPrim !== 'undefined') {
		var result = exoticToPrim.call(input, hint);
		if (isPrimitive(result)) {
			return result;
		}
		throw new $TypeError('unable to convert exotic object to primitive');
	}
	if (hint === 'default' && (isDate(input) || isSymbol(input))) {
		hint = /** @type {const} */ ('string');
	}

	return OrdinaryToPrimitive(input, hint === 'default' ? 'number' : hint);
};
