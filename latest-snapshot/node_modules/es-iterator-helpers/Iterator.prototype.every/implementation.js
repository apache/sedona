'use strict';

var $TypeError = require('es-errors/type');

var Call = require('es-abstract/2025/Call');
var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IfAbruptCloseIterator = require('es-abstract/2025/IfAbruptCloseIterator');
var IsCallable = require('es-abstract/2025/IsCallable');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var NormalCompletion = require('es-abstract/2025/NormalCompletion');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');
var ToBoolean = require('es-abstract/2025/ToBoolean');

var isObject = require('es-abstract/helpers/isObject');

module.exports = function every(predicate) {
	if (this instanceof every) {
		throw new $TypeError('`every` is not a constructor');
	}

	var O = this; // step 1
	if (!isObject(O)) {
		throw new $TypeError('`this` value must be an Object'); // step 2
	}

	var iterated = { // step 3
		'[[Iterator]]': O,
		'[[NextMethod]]': undefined,
		'[[Done]]': false
	};

	if (!IsCallable(predicate)) { // step 4
		var error = ThrowCompletion(new $TypeError('`predicate` must be a function')); // step 4.a
		return IteratorClose(iterated, error); // step 4.b
	}

	iterated = GetIteratorDirect(O); // step 5

	var counter = 0; // step 6

	while (true) { // step 7
		var value = IteratorStepValue(iterated); // step 7.a
		if (iterated['[[Done]]']) {
			return true; // step 7.b
		}
		var result;
		try {
			result = Call(predicate, void undefined, [value, counter]); // step 7.c
		} catch (e) {
			return IfAbruptCloseIterator(ThrowCompletion(e), iterated); // step 7.d
		}

		if (!ToBoolean(result)) {
			return IteratorClose(
				iterated,
				NormalCompletion(false)
			); // step 7.e
		}

		counter += 1; // step 7.f
	}
};
