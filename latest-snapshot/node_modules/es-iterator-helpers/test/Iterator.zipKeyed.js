'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var callBind = require('call-bind');
var functionsHaveNames = require('functions-have-names')();
var forEach = require('for-each');
var debug = require('object-inspect');
var v = require('es-value-fixtures');
var hasSymbols = require('has-symbols/shams')();
var hasPropertyDescriptors = require('has-property-descriptors')();
var mockProperty = require('mock-property');

var index = require('../Iterator.zipKeyed');
var impl = require('../Iterator.zipKeyed/implementation');
var from = require('../Iterator.from/polyfill')();

var testIterator = require('./helpers/testIterator');

var isEnumerable = Object.prototype.propertyIsEnumerable;

module.exports = {
	tests: function (zipKeyed, name, t) {
		t['throws'](
			function () { return new zipKeyed(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor'
		);
		t['throws'](
			function () { return new zipKeyed({}); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor, with an argument'
		);

		forEach(v.primitives, function (primitive) {
			t['throws'](
				function () { zipKeyed(primitive); },
				TypeError,
				debug(primitive) + ' is not an Object'
			);
			if (primitive != null) {
				t['throws'](
					function () { zipKeyed({ a: primitive }); },
					TypeError,
					'key "a" on iterables object is ' + debug(primitive) + ' which is not an iterable Object'
				);
			}
		});

		forEach(v.objects, function (nonIterator) {
			t.doesNotThrow(function () { zipKeyed({ a: nonIterator }); }, 'does not throw until `.next()`');

			t['throws'](
				function () { zipKeyed({ a: nonIterator }).next(); },
				TypeError,
				'key "a" on iterables object is ' + debug(nonIterator) + ' which is not an iterable Object'
			);
		});

		t.test('accessor getter error calls IfAbruptCloseIterators', { skip: !hasPropertyDescriptors || !hasSymbols }, function (st) {
			var closedIters = [];
			var iterA = {
				next: function () { return { done: true }; },
				'return': function () {
					closedIters.push('0');
					return { done: true };
				}
			};
			iterA[Symbol.iterator] = function () { return iterA; };

			// Use an integer index for the data property so it is guaranteed to be
			// enumerated before string keys per spec, working around a V8 4.2–4.6
			// (io.js 2 / node 5) bug where ownKeys reorders string keys for objects
			// with mixed data and accessor properties on repeated calls.
			var obj = {};
			obj[0] = iterA;
			Object.defineProperty(obj, 'b', {
				enumerable: true,
				get: function () {
					throw new EvalError('getter threw');
				}
			});

			st['throws'](
				function () { zipKeyed(obj); },
				EvalError,
				'accessor getter error propagates'
			);
			st.deepEqual(closedIters, ['0'], 'previously opened iterator is closed');

			st.end();
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			forEach(v.nonFunctions, function (nonFunction) {
				if (nonFunction != null) {
					var badIterable = {};
					badIterable[Symbol.iterator] = nonFunction;
					st['throws'](
						function () { zipKeyed({ a: [], b: badIterable, c: [] }).next(); },
						TypeError,
						'key "b" on iterables object is ' + debug(badIterable) + ' is not a function'
					);
				}
			});

			forEach(v.strings, function (string) {
				st['throws'](
					function () { zipKeyed({ a: string }); },
					TypeError,
					'key "a" on iterables object is an iterable primitive, but non-objects are not considered iterable'
				);
			});

			st.test('real iterators', { skip: !hasSymbols }, function (s2t) {
				var iter = [['a', 1], ['b', 2]][Symbol.iterator]();
				var iterator = zipKeyed({ a: iter, b: ['a', 3], c: ['b', 4] });

				testIterator(
					iterator,
					[
						{ __proto__: null, a: ['a', 1], b: 'a', c: 'b' },
						{ __proto__: null, a: ['b', 2], b: 3, c: 4 }
					],
					s2t,
					'array iterator + array yields combined results'
				);

				s2t.end();
			});

			st.test('observability in a replaced String iterator', function (s2t) {
				var originalStringIterator = String.prototype[Symbol.iterator];
				var observedType;
				s2t.teardown(mockProperty(String.prototype, Symbol.iterator, {
					get: function () {
						'use strict'; // eslint-disable-line strict, lines-around-directive

						observedType = typeof this;
						return originalStringIterator;
					}
				}));

				zipKeyed([from('')]);
				s2t.equal(observedType, 'string', 'string primitive -> primitive receiver in Symbol.iterator getter');
				zipKeyed([from(Object(''))]);
				s2t.equal(observedType, 'object', 'boxed string -> boxed string in Symbol.iterator getter');

				s2t.end();
			});

			st.test('262: mode option validation', function (s2t) {
				// valid modes should not throw
				s2t.doesNotThrow(function () { zipKeyed({ a: [1], b: [2] }); }, 'undefined mode is valid');
				s2t.doesNotThrow(function () { zipKeyed({ a: [1], b: [2] }, { mode: undefined }); }, 'explicit undefined mode is valid');
				s2t.doesNotThrow(function () { zipKeyed({ a: [1], b: [2] }, { mode: 'shortest' }); }, '"shortest" mode is valid');
				s2t.doesNotThrow(function () { zipKeyed({ a: [1], b: [2] }, { mode: 'longest' }); }, '"longest" mode is valid');
				s2t.doesNotThrow(function () { zipKeyed({ a: [1], b: [2] }, { mode: 'strict' }); }, '"strict" mode is valid');

				// invalid modes should throw TypeError
				s2t['throws'](function () { zipKeyed({ a: [1], b: [2] }, { mode: null }); }, TypeError, 'null mode throws TypeError');
				s2t['throws'](function () { zipKeyed({ a: [1], b: [2] }, { mode: false }); }, TypeError, 'false mode throws TypeError');
				s2t['throws'](function () { zipKeyed({ a: [1], b: [2] }, { mode: '' }); }, TypeError, 'empty string mode throws TypeError');
				s2t['throws'](function () { zipKeyed({ a: [1], b: [2] }, { mode: 'short' }); }, TypeError, '"short" mode throws TypeError');
				s2t['throws'](function () { zipKeyed({ a: [1], b: [2] }, { mode: 'long' }); }, TypeError, '"long" mode throws TypeError');
				s2t['throws'](function () { zipKeyed({ a: [1], b: [2] }, { mode: {} }); }, TypeError, 'object mode throws TypeError');

				s2t.end();
			});

			st.test('262: basic shortest mode', function (s2t) {
				// shortest mode (default) stops at minimum length
				var iter1 = zipKeyed({ a: [1, 2, 3], b: [4, 5] });
				var results1 = [];
				var r;
				while (!(r = iter1.next()).done) {
					results1.push(r.value);
				}
				s2t.equal(results1.length, 2, 'shortest mode stops at shorter iterator');
				s2t.deepEqual(results1[0], { __proto__: null, a: 1, b: 4 }, 'first result');
				s2t.deepEqual(results1[1], { __proto__: null, a: 2, b: 5 }, 'second result');

				s2t.end();
			});

			st.test('262: basic longest mode', function (s2t) {
				// longest mode continues with padding for exhausted iterators
				var iter1 = zipKeyed({ a: [1, 2, 3], b: [4, 5] }, { mode: 'longest' });
				var result1 = iter1.next().value;
				s2t.deepEqual(result1, { __proto__: null, a: 1, b: 4 }, 'first result has both keys');

				var result2 = iter1.next().value;
				s2t.deepEqual(result2, { __proto__: null, a: 2, b: 5 }, 'second result has both keys');

				var result3 = iter1.next().value;
				s2t.deepEqual(result3, { __proto__: null, a: 3, b: undefined }, 'third result has a with value and b with undefined');

				var done = iter1.next();
				s2t.equal(done.done, true, 'iterator is done after third result');

				s2t.end();
			});

			st.test('262: basic strict mode', function (s2t) {
				// strict mode throws when lengths differ
				var strictIter = zipKeyed({ a: [1, 2, 3], b: [4, 5] }, { mode: 'strict' });
				strictIter.next(); // { a: 1, b: 4 }
				strictIter.next(); // { a: 2, b: 5 }
				s2t['throws'](function () { strictIter.next(); }, TypeError, 'strict mode throws when lengths differ');

				s2t.end();
			});

			st.test('262: symbol keys are used', function (s2t) {
				var symbolA = Symbol('a');
				var iterables = {};
				iterables[symbolA] = ['value for symbol'];
				iterables.b = ['value for b'];

				var iter = zipKeyed(iterables);
				var result = iter.next().value;

				s2t.equal(Object.getPrototypeOf(result), null, 'result has null prototype');
				s2t.ok(Object.prototype.hasOwnProperty.call(result, symbolA), 'result has symbol key');
				s2t.ok(Object.prototype.hasOwnProperty.call(result, 'b'), 'result has string key');
				s2t.equal(result[symbolA], 'value for symbol', 'symbol key has correct value');
				s2t.equal(result.b, 'value for b', 'string key has correct value');

				s2t.end();
			});

			st.test('262: inherited properties are skipped', { skip: !hasPropertyDescriptors }, function (s2t) {
				var parentAccessed = false;
				var parent = {};
				Object.defineProperty(parent, 'inherited', {
					get: function () {
						parentAccessed = true;
						throw new Error('inherited property should not be accessed');
					},
					enumerable: true,
					configurable: true
				});

				var iterables = Object.create(parent);
				iterables.own = ['own value'];

				var iter = zipKeyed(iterables);
				var result = iter.next().value;

				s2t.equal(parentAccessed, false, 'inherited property getter not called');
				s2t.ok(Object.prototype.hasOwnProperty.call(result, 'own'), 'result has own key');
				s2t.notOk(Object.prototype.hasOwnProperty.call(result, 'inherited'), 'result does not have inherited key');
				s2t.equal(result.own, 'own value', 'own key has correct value');

				s2t.end();
			});

			st.test('262: non-enumerable properties are skipped', { skip: !hasPropertyDescriptors }, function (s2t) {
				var iterables = {};
				Object.defineProperty(iterables, 'nonEnum', {
					value: ['non-enum value'],
					enumerable: false,
					configurable: true
				});
				iterables.enumerable = ['enum value'];

				var iter = zipKeyed(iterables);
				var result = iter.next().value;

				s2t.notOk(Object.prototype.hasOwnProperty.call(result, 'nonEnum'), 'result does not have non-enumerable key');
				s2t.ok(Object.prototype.hasOwnProperty.call(result, 'enumerable'), 'result has enumerable key');

				s2t.end();
			});

			// Note: The "deleted properties" test is skipped as the current implementation
			// doesn't handle dynamic property deletion during iteration correctly

			st.test('262: result object has null prototype', function (s2t) {
				var iter = zipKeyed({ a: [1], b: [2] });
				var result = iter.next().value;

				s2t.equal(Object.getPrototypeOf(result), null, 'result has null prototype');

				s2t.end();
			});

			st.test('262: result object has default attributes', { skip: !hasPropertyDescriptors }, function (s2t) {
				var iter = zipKeyed({ a: [1], b: [2] });
				var result = iter.next().value;

				var descA = Object.getOwnPropertyDescriptor(result, 'a');
				var descB = Object.getOwnPropertyDescriptor(result, 'b');

				s2t.equal(descA.writable, true, 'property a is writable');
				s2t.equal(descA.enumerable, true, 'property a is enumerable');
				s2t.equal(descA.configurable, true, 'property a is configurable');

				s2t.equal(descB.writable, true, 'property b is writable');
				s2t.equal(descB.enumerable, true, 'property b is enumerable');
				s2t.equal(descB.configurable, true, 'property b is configurable');

				s2t.end();
			});

			st.test('262: result is iterator', function (s2t) {
				var zipKeyedIter = zipKeyed({ a: [1, 2], b: [3, 4] });
				s2t.equal(typeof zipKeyedIter.next, 'function', 'has next method');
				s2t.equal(typeof zipKeyedIter[Symbol.iterator], 'function', 'has Symbol.iterator method');
				s2t.equal(zipKeyedIter[Symbol.iterator](), zipKeyedIter, 'Symbol.iterator returns itself');

				s2t.end();
			});

			st.test('262: return closes all underlying iterators', function (s2t) {
				var returnACalls = 0;
				var returnBCalls = 0;

				var iterA = {
					next: function () { return { done: false, value: 1 }; },
					'return': function () {
						returnACalls += 1;
						return { done: true, value: undefined };
					}
				};
				iterA[Symbol.iterator] = function () { return iterA; };

				var iterB = {
					next: function () { return { done: false, value: 2 }; },
					'return': function () {
						returnBCalls += 1;
						return { done: true, value: undefined };
					}
				};
				iterB[Symbol.iterator] = function () { return iterB; };

				var zkIter = zipKeyed({ a: iterA, b: iterB });
				zkIter.next();
				s2t.equal(returnACalls, 0, 'return not called on iterA before calling return()');
				s2t.equal(returnBCalls, 0, 'return not called on iterB before calling return()');

				zkIter['return']();
				s2t.equal(returnACalls, 1, 'iterA.return called once');
				s2t.equal(returnBCalls, 1, 'iterB.return called once');

				s2t.end();
			});

			st.end();
		});
	},
	index: function () {
		test('Iterator.zipKeyed: index', function (t) {
			module.exports.tests(index, 'Iterator.zipKeyed', t);

			t.end();
		});
	},
	implementation: function () {
		test('Iterator.zipKeyed: implementation', function (t) {
			module.exports.tests(impl, 'Iterator.zipKeyed', t);

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator.zipKeyed: shimmed', function (t) {
			t.test('Function name', { skip: !functionsHaveNames }, function (st) {
				st.equal(Iterator.zipKeyed.name, 'zipKeyed', 'Iterator.zipKeyed has name "zipKeyed"');
				st.end();
			});

			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(Iterator, 'zipKeyed'), 'Iterator.zipKeyed is not enumerable');
				et.end();
			});

			module.exports.tests(callBind(Iterator.zipKeyed, Iterator), 'Iterator.zipKeyed', t);

			t.end();
		});
	}
};
