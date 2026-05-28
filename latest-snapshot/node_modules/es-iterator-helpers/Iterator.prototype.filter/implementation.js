'use strict';

var $TypeError = require('es-errors/type');

var Call = require('es-abstract/2025/Call');
var CreateIteratorFromClosure = require('es-abstract/2025/CreateIteratorFromClosure');
var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IfAbruptCloseIterator = require('es-abstract/2025/IfAbruptCloseIterator');
var IsCallable = require('es-abstract/2025/IsCallable');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');
var ToBoolean = require('es-abstract/2025/ToBoolean');

var isObject = require('es-abstract/helpers/isObject');

var SLOT = require('internal-slot');
var iterHelperProto = require('../IteratorHelperPrototype');

module.exports = function filter(predicate) {
	if (this instanceof filter) {
		throw new $TypeError('`filter` is not a constructor');
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

	var closeIfAbrupt = function (abruptCompletion) {
		IfAbruptCloseIterator(abruptCompletion, iterated);
	};

	var sentinel = {};
	var counter = 0; // step 6.a
	var yielded = false;
	var closure = function () {
		while (true) { // step 6.b
			if (yielded) {
				counter += 1; // step 6.b.vi - deferred from after Yield
				yielded = false;
			}
			var value = IteratorStepValue(iterated); // step 6.b.i
			if (iterated['[[Done]]']) {
				return sentinel; // step 6.b.ii
			}

			var selected;
			try {
				selected = Call(predicate, void undefined, [value, counter]); // step 6.b.iii
			} catch (e) {
				return IfAbruptCloseIterator(ThrowCompletion(e), iterated); // step 6.b.iv
			}
			if (ToBoolean(selected)) { // step 6.b.v
				yielded = true;
				return value; // step 6.b.v.1 - Yield(value)
			}
			counter += 1; // step 6.b.vi
		}
	};
	SLOT.set(closure, '[[Sentinel]]', sentinel); // for the userland implementation
	SLOT.set(closure, '[[CloseIfAbrupt]]', closeIfAbrupt); // for the userland implementation

	var result = CreateIteratorFromClosure(closure, 'Iterator Helper', iterHelperProto, ['[[UnderlyingIterators]]']); // step 7

	SLOT.set(result, '[[UnderlyingIterators]]', [iterated]); // step 8

	return result; // step 9
};
