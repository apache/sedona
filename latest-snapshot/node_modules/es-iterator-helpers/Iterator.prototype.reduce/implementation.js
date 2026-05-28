'use strict';

var $TypeError = require('es-errors/type');

var Call = require('es-abstract/2025/Call');
var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IfAbruptCloseIterator = require('es-abstract/2025/IfAbruptCloseIterator');
var IsCallable = require('es-abstract/2025/IsCallable');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');

var isObject = require('es-abstract/helpers/isObject');

module.exports = function reduce(reducer) {
	if (this instanceof reduce) {
		throw new $TypeError('`reduce` is not a constructor');
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

	if (!IsCallable(reducer)) { // step 4
		var error = ThrowCompletion(new $TypeError('`reducer` must be a function')); // step 4.a
		return IteratorClose(iterated, error); // step 4.b
	}

	iterated = GetIteratorDirect(O); // step 5

	var accumulator;
	var counter;
	if (arguments.length < 2) { // step 6
		accumulator = IteratorStepValue(iterated); // step 6.a
		if (iterated['[[Done]]']) {
			throw new $TypeError('Reduce of empty iterator with no initial value'); // step 6.b
		}
		counter = 1; // step 6.c
	} else { // step 7
		accumulator = arguments[1]; // step 7.a
		counter = 0; // step 7.b
	}

	while (true) { // step 8
		var value = IteratorStepValue(iterated); // step 8.a
		if (iterated['[[Done]]']) {
			return accumulator; // step 8.b
		}
		try {
			var result = Call(reducer, void undefined, [accumulator, value, counter]); // step 8.c
			accumulator = result; // step 8.e
		} catch (e) {
			return IfAbruptCloseIterator(ThrowCompletion(e), iterated); // step 8.d
		}
		counter += 1; // step 8.f
	}
};
