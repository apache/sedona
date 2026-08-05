'use strict';

var $RangeError = require('es-errors/range');
var $TypeError = require('es-errors/type');

var CompletionRecord = require('es-abstract/2025/CompletionRecord');
var CreateIteratorFromClosure = require('es-abstract/2025/CreateIteratorFromClosure');
var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStep = require('es-abstract/2025/IteratorStep');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');
var ToIntegerOrInfinity = require('es-abstract/2025/ToIntegerOrInfinity');
var ToNumber = require('es-abstract/2025/ToNumber');

var iterHelperProto = require('../IteratorHelperPrototype');

var isObject = require('es-abstract/helpers/isObject');
var isNaN = require('es-abstract/helpers/isNaN');

var SLOT = require('internal-slot');

module.exports = function drop(limit) {
	if (this instanceof drop) {
		throw new $TypeError('`drop` is not a constructor');
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

	var numLimit;
	try {
		numLimit = ToNumber(limit); // step 4
	} catch (e) {
		return IteratorClose(iterated, ThrowCompletion(e)); // step 5
	}

	if (isNaN(numLimit)) { // step 6
		return IteratorClose(iterated, ThrowCompletion(new $RangeError('`limit` must be a non-NaN number')));
	}

	var integerLimit = ToIntegerOrInfinity(numLimit); // step 7
	if (integerLimit < 0) { // step 8
		return IteratorClose(iterated, ThrowCompletion(new $RangeError('`limit` must be >= 0')));
	}

	iterated = GetIteratorDirect(O); // step 9

	var closeIfAbrupt = function (abruptCompletion) {
		if (!(abruptCompletion instanceof CompletionRecord)) {
			throw new $TypeError('`abruptCompletion` must be a Completion Record');
		}
		IteratorClose(
			iterated,
			abruptCompletion
		);
	};

	var sentinel = {};
	var remaining = integerLimit; // step 10.a
	var closure = function () { // step 10
		var next;
		while (remaining > 0) { // step 10.b
			if (remaining !== Infinity) { // step 10.b.i
				remaining -= 1; // step 10.b.i.1
			}

			next = IteratorStep(iterated); // step 10.b.ii
			if (!next) {
				// return void undefined; // step 10.b.iii
				return sentinel;
			}
		}
		// while (true) { // step 10.c
		var value = IteratorStepValue(iterated); // step 10.c.i - ? means throw on protocol violation, don't close
		if (iterated['[[Done]]']) {
			return sentinel; // step 10.c.ii
		}
		return value; // step 10.c.iii - Yield(value)
		// }
		// return void undefined;
	};
	SLOT.set(closure, '[[Sentinel]]', sentinel); // for the userland implementation
	SLOT.set(closure, '[[CloseIfAbrupt]]', closeIfAbrupt); // for the userland implementation

	var result = CreateIteratorFromClosure(closure, 'Iterator Helper', iterHelperProto, ['[[UnderlyingIterators]]']); // step 11

	SLOT.set(result, '[[UnderlyingIterators]]', [iterated]); // step 12

	return result; // step 13
};
