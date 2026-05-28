'use strict';

var $TypeError = require('es-errors/type');

var CreateDataPropertyOrThrow = require('es-abstract/2025/CreateDataPropertyOrThrow');
var Get = require('es-abstract/2025/Get');
var GetIteratorFlattenable = require('es-abstract/2025/GetIteratorFlattenable');
var OrdinaryObjectCreate = require('es-abstract/2025/OrdinaryObjectCreate');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');
var ToPropertyDescriptor = require('es-abstract/2025/ToPropertyDescriptor');

var forEach = require('es-abstract/helpers/forEach');
var isObject = require('es-abstract/helpers/isObject');
var ownKeys = require('es-abstract/helpers/OwnPropertyKeys');

var gOPD = require('gopd');
var IteratorZip = require('../aos/IteratorZip');
var IfAbruptCloseIterators = require('../aos/IfAbruptCloseIterators');
var GetOptionsObject = require('../aos/GetOptionsObject');

module.exports = function zipKeyed(iterables) {
	if (this instanceof zipKeyed) {
		throw new $TypeError('`Iterator.zip` is not a constructor');
	}

	if (!isObject(iterables)) {
		throw new $TypeError('`iterables` must be an Object'); // step 1
	}

	var options = GetOptionsObject(arguments.length > 1 ? arguments[1] : void undefined); // step 2

	var mode = Get(options, 'mode'); // step 3

	if (typeof mode === 'undefined') {
		mode = 'shortest'; // step 4
	}

	if (mode !== 'shortest' && mode !== 'longest' && mode !== 'strict') {
		throw new $TypeError('`mode` must be one of "shortest", "longest", or "strict"'); // step 5
	}

	var paddingOption; // step 6

	if (mode === 'longest') { // step 7
		paddingOption = Get(options, 'padding'); // step 7.a
		if (typeof paddingOption !== 'undefined' && !isObject(paddingOption)) {
			throw new $TypeError('`padding` option must be an Object'); // step 7.b
		}
	}

	var iters = []; // step 8

	var padding = []; // step 9

	var allKeys = ownKeys(iterables); // step 10

	var keys = []; // step 11

	// eslint-disable-next-line consistent-return
	forEach(allKeys, function (key) { // step 12
		var desc;
		try {
			desc = ToPropertyDescriptor(gOPD(iterables, key)); // step 12.a
		} catch (e) {
			return IfAbruptCloseIterators(ThrowCompletion(e), iters); // step 12.b
		}

		if (typeof desc !== 'undefined' && desc['[[Enumerable]]'] === true) { // step 12.c
			var value;
			try {
				value = Get(iterables, key); // step 12.c.i
			} catch (e) {
				return IfAbruptCloseIterators(ThrowCompletion(e), iters); // step 12.c.ii
			}
			if (typeof value !== 'undefined') { // step 12.c.iii
				keys[keys.length] = key; // step 12.c.iii.1
				var iter;
				try {
					iter = GetIteratorFlattenable(value, 'REJECT-PRIMITIVES'); // step 12.c.iii.2
				} catch (e) {
					return IfAbruptCloseIterators(ThrowCompletion(e), iters); // step 12.c.iii.3
				}
				iters[iters.length] = iter; // step 12.c.iii.4
			}
		}
	});

	var iterCount = iters.length; // step 13

	if (mode === 'longest') { // step 14
		if (typeof paddingOption === 'undefined') { // step 14.a
			for (var j = 0; j < iterCount; j += 1) { // step 14.a.i
				padding[padding.length] = void undefined; // step 14.a.i.1
			}
		} else { // step 14.b
			// eslint-disable-next-line consistent-return
			forEach(keys, function (key) { // step 14.b.i
				var value;
				try {
					value = Get(paddingOption, key); // step 14.b.i.1
				} catch (e) {
					return IfAbruptCloseIterators(ThrowCompletion(e), iters); // step 14.b.i.2
				}
				padding[padding.length] = value; // step 14.b.i.3
			});
		}
	}

	// eslint-disable-next-line no-sequences
	var finishResults = (0, function (results) { // step 15
		var obj = OrdinaryObjectCreate(null); // step 15.a
		for (var i = 0; i < iterCount; i += 1) { // step 15.b
			CreateDataPropertyOrThrow(obj, keys[i], results[i]); // step 15.b.i
		}
		return obj; // step 15.c
	});

	return IteratorZip(iters, mode, padding, finishResults); // step 16
};
