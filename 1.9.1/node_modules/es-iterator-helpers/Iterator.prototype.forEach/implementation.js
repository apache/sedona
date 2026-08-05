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

module.exports = function forEach(procedure) {
	if (this instanceof forEach) {
		throw new $TypeError('`forEach` is not a constructor');
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

	if (!IsCallable(procedure)) { // step 4
		var error = ThrowCompletion(new $TypeError('`fn` must be a function')); // step 4.a
		return IteratorClose(iterated, error); // step 4.b
	}

	iterated = GetIteratorDirect(O); // step 5

	var counter = 0; // step 6

	while (true) { // step 7
		var value = IteratorStepValue(iterated); // step 7.a
		if (iterated['[[Done]]']) {
			return void undefined; // step 7.b
		}
		try {
			Call(procedure, void undefined, [value, counter]); // step 7.c
		} catch (e) {
			return IfAbruptCloseIterator(ThrowCompletion(e), iterated); // steps 7.d
		}

		counter += 1; // step 7.e
	}
};
