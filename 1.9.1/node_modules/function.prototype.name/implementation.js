'use strict';

var IsCallable = require('is-callable');
var hasOwn = require('hasown');
var functionsHaveNames = require('functions-have-names')();
var $TypeError = require('es-errors/type');
var callBound = require('call-bound');
var isDDA = require('is-document.all');

var $functionToString = callBound('Function.prototype.toString');
var $stringMatch = callBound('String.prototype.match');

var classRegex = /^class /;

/** @param {unknown} fn */
var isClass = function isClassConstructor(fn) {
	if (IsCallable(fn)) {
		return false;
	}
	if (typeof fn !== 'function') {
		return false;
	}
	try {
		var match = $stringMatch($functionToString(fn), classRegex);
		return !!match;
	} catch (e) {}
	return false;
};

var regex = /\s*function\s+([^(\s]*)\s*/;

var functionProto = Function.prototype;

/** @type {import('./implementation')} */
module.exports = function getName() {
	if (isDDA(this) || (!isClass(this) && !IsCallable(this))) {
		throw new $TypeError('Function.prototype.name sham getter called on non-function');
	}
	if (functionsHaveNames && hasOwn(this, 'name')) {
		return this.name;
	}
	if (this === functionProto) {
		return '';
	}
	var str = $functionToString(this);
	var match = $stringMatch(str, regex);
	var name = match && match[1];

	return /** @type {string} */ (name);
};
