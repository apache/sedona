'use strict';

var $TypeError = require('es-errors/type');

var Call = require('es-abstract/2025/Call');
var CompletionRecord = require('es-abstract/2025/CompletionRecord');
var CreateIteratorFromClosure = require('es-abstract/2025/CreateIteratorFromClosure');
var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var GetIteratorFlattenable = require('es-abstract/2025/GetIteratorFlattenable');
var IsCallable = require('es-abstract/2025/IsCallable');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');

var isObject = require('es-abstract/helpers/isObject');

var iterHelperProto = require('../IteratorHelperPrototype');

var SLOT = require('internal-slot');

module.exports = function flatMap(mapper) {
	if (this instanceof flatMap) {
		throw new $TypeError('`flatMap` is not a constructor');
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

	if (!IsCallable(mapper)) { // step 4
		return IteratorClose(iterated, ThrowCompletion(new $TypeError('`mapper` must be a function')));
	}

	iterated = GetIteratorDirect(O); // step 5

	var sentinel = { sentinel: true };
	var innerIterator = sentinel;

	var closeIfAbrupt = function (abruptCompletion) {
		if (!(abruptCompletion instanceof CompletionRecord)) {
			throw new $TypeError('`abruptCompletion` must be a Completion Record');
		}
		if (innerIterator !== sentinel) {
			var backupCompletion;
			try {
				IteratorClose(innerIterator, abruptCompletion);
			} catch (e) {
				backupCompletion = ThrowCompletion(e);
			}
			innerIterator = sentinel;
			if (backupCompletion) {
				IteratorClose(iterated, backupCompletion);
			}
			IteratorClose(iterated, abruptCompletion);
		} else {
			IteratorClose(iterated, abruptCompletion);
		}
	};

	var counter = 0; // step 6.a
	var innerAlive = false;
	var closure = function () {
		// while (true) { // step 6.b
		if (innerIterator === sentinel) {
			var value = IteratorStepValue(iterated); // step 6.b.i
			if (iterated['[[Done]]']) {
				innerAlive = false;
				innerIterator = sentinel;
				// return void undefined; // step 6.b.ii
				return sentinel;
			}
		}

		if (innerIterator === sentinel) {
			innerAlive = true; // step 6.b.vii
			try {
				var mapped = Call(mapper, void undefined, [value, counter]); // step 6.b.iii
				// yield mapped // step 6.b.v
				innerIterator = GetIteratorFlattenable(mapped, 'REJECT-PRIMITIVES'); // step 6.b.v
			} catch (e) {
				innerAlive = false;
				innerIterator = sentinel;
				closeIfAbrupt(ThrowCompletion(e)); // steps 6.b.iv, 6.b.vi
			} finally {
				counter += 1; // step 6.b.ix
			}
		}
		// while (innerAlive) { // step 6.b.viii
		if (innerAlive) {
			// step 6.b.viii.4
			var innerValue;
			try {
				innerValue = IteratorStepValue(innerIterator); // step 6.b.viii.1
			} catch (e) {
				// inner iterator violated the protocol (IteratorStepValue threw), so don't close inner
				// but DO close outer iterated per spec step 6.b.viii.2: IfAbruptCloseIterator(innerValue, iterated)
				innerAlive = false;
				innerIterator = sentinel;
				IteratorClose(iterated, ThrowCompletion(e)); // step 6.b.viii.2 - close outer only
			}
			if (innerIterator['[[Done]]']) {
				innerAlive = false;
				innerIterator = sentinel;
				return closure();
			}
			return innerValue; // step 6.b.viii.4.1
		}
		// }
		// return void undefined;
		return sentinel;
	};
	SLOT.set(closure, '[[Sentinel]]', sentinel); // for the userland implementation
	SLOT.set(closure, '[[CloseIfAbrupt]]', closeIfAbrupt); // for the userland implementation

	var result = CreateIteratorFromClosure(closure, 'Iterator Helper', iterHelperProto, ['[[UnderlyingIterators]]']); // step 7

	SLOT.set(result, '[[UnderlyingIterators]]', [iterated]); // step 8

	return result; // step 9
};
