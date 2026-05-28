'use strict';

var $RangeError = require('es-errors/range');
var $TypeError = require('es-errors/type');

var CompletionRecord = require('es-abstract/2025/CompletionRecord');
var CreateIteratorFromClosure = require('es-abstract/2025/CreateIteratorFromClosure');
var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var NormalCompletion = require('es-abstract/2025/NormalCompletion');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');
var ToIntegerOrInfinity = require('es-abstract/2025/ToIntegerOrInfinity');
var ToNumber = require('es-abstract/2025/ToNumber');

var iterHelperProto = require('../IteratorHelperPrototype');

var isObject = require('es-abstract/helpers/isObject');
var isNaN = require('es-abstract/helpers/isNaN');

var SLOT = require('internal-slot');

module.exports = function take(limit) {
	if (this instanceof take) {
		throw new $TypeError('`take` is not a constructor');
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
		// while (true) { // step 10.b
		if (remaining === 0) { // step 10.b.i
			return IteratorClose( // step 10.b.i.1
				iterated,
				NormalCompletion(sentinel)
			);
		}
		if (remaining !== Infinity) { // step 10.b.ii
			remaining -= 1; // step 10.b.ii.1
		}

		var value = IteratorStepValue(iterated); // step 10.b.iii
		if (iterated['[[Done]]']) {
			return sentinel; // step 10.b.iv
		}

		return value; // step 10.b.v
		// }
	};
	SLOT.set(closure, '[[Sentinel]]', sentinel); // for the userland implementation
	SLOT.set(closure, '[[CloseIfAbrupt]]', closeIfAbrupt); // for the userland implementation

	var result = CreateIteratorFromClosure(closure, 'Iterator Helper', iterHelperProto, ['[[UnderlyingIterators]]']); // step 11

	SLOT.set(result, '[[UnderlyingIterators]]', [iterated]); // step 12

	return result; // step 13
};
