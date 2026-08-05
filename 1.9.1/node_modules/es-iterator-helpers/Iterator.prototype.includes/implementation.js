'use strict';

var $RangeError = require('es-errors/range');
var $TypeError = require('es-errors/type');

var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IteratorClose = require('es-abstract/2025/IteratorClose');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');
var NormalCompletion = require('es-abstract/2025/NormalCompletion');
var SameValueZero = require('es-abstract/2025/SameValueZero');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');

var isInteger = require('math-intrinsics/isInteger');
var MAX_SAFE_INTEGER = require('math-intrinsics/constants/maxSafeInteger');

var isFinite = require('es-abstract/helpers/isFinite');
var isNaN = require('es-abstract/helpers/isNaN');
var isObject = require('es-abstract/helpers/isObject');

module.exports = function includes(searchElement) {
	if (this instanceof includes) {
		throw new $TypeError('`includes` is not a constructor');
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

	var skippedElements = arguments.length > 1 ? arguments[1] : undefined;

	var toSkip = 0; // step 4
	if (typeof skippedElements !== 'undefined') { // step 5
		if (
			typeof skippedElements !== 'number'
			|| isNaN(skippedElements)
			|| (isFinite(skippedElements) && !isInteger(skippedElements))
		) { // step 5.a
			var error = ThrowCompletion(new $TypeError('`skippedElements` must be an integral Number, +Infinity, or -Infinity')); // step 5.a.i
			return IteratorClose(iterated, error); // step 5.a.ii
		}

		toSkip = skippedElements; // step 5.b
	}

	if (toSkip < 0) { // step 6
		var error2 = ThrowCompletion(new $RangeError('`skippedElements` must be >= 0')); // step 6.a
		return IteratorClose(iterated, error2); // step 6.b
	}

	if (isFinite(toSkip) && toSkip > MAX_SAFE_INTEGER) { // step 7
		var error3 = ThrowCompletion(new $RangeError('`skippedElements` must be <= 2 ** 53 - 1')); // step 7.a
		return IteratorClose(iterated, error3); // step 7.b
	}

	var skipped = 0; // step 8

	iterated = GetIteratorDirect(O); // step 9

	while (true) { // step 10
		var value = IteratorStepValue(iterated); // step 10.a

		if (iterated['[[Done]]']) {
			return false; // step 10.b
		}
		if (skipped < toSkip) { // step 10.c
			skipped += 1; // step 10.c.i
		} else if (SameValueZero(value, searchElement)) { // step 10.d
			return IteratorClose(
				iterated,
				NormalCompletion(true)
			); // step 10.d.i
		}
	}
};
