'use strict';

var $TypeError = require('es-errors/type');

var isCallable = require('is-callable');

var inspect = require('object-inspect');

var GetV = require('./GetV');
var isPropertyKey = require('./isPropertyKey');

// https://262.ecma-international.org/6.0/#sec-getmethod

/** @type {import('./GetMethod')} */
module.exports = function GetMethod(O, P) {
	// 7.3.9.1
	if (!isPropertyKey(P)) {
		throw new $TypeError('Assertion failed: P is not a Property Key');
	}

	// 7.3.9.2
	var func = GetV(O, P);

	// 7.3.9.4
	if (func == null) {
		return void 0;
	}

	// 7.3.9.5
	if (!isCallable(func)) {
		throw new $TypeError(inspect(P) + ' is not a function: ' + inspect(func));
	}

	// 7.3.9.6
	return func;
};
