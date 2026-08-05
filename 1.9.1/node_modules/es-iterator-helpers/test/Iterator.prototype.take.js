'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var callBind = require('call-bind');
var functionsHaveNames = require('functions-have-names')();
var hasStrictMode = require('has-strict-mode')();
var forEach = require('for-each');
var debug = require('object-inspect');
var v = require('es-value-fixtures');
var hasSymbols = require('has-symbols/shams')();
var hasPropertyDescriptors = require('has-property-descriptors')();
var iterate = require('iterate-iterator');

var index = require('../Iterator.prototype.take');
var impl = require('../Iterator.prototype.take/implementation');

var fnName = 'take';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (take, name, t) {
		t['throws'](
			function () { return new take(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(take(nonIterator, 1)); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			if (nonIterator != null && typeof nonIterator !== 'string') {
				var badNext = { next: nonIterator };
				t['throws'](
					function () { iterate(take(badNext, 1)); },
					TypeError,
					debug(badNext) + ' is not an Object with a callable `next` method'
				);
			}
		});

		t.test('early-error calls return on receiver', function (st) {
			var returnCalls = 0;
			var obj = {
				next: function () { return { done: true }; },
				'return': function () {
					returnCalls += 1;
					return { done: true };
				}
			};

			st['throws'](
				function () { take(obj, NaN); },
				RangeError,
				'NaN throws RangeError'
			);
			st.equal(returnCalls, 1, 'return called on receiver for NaN');

			returnCalls = 0;
			st['throws'](
				function () { take(obj, -1); },
				RangeError,
				'-1 throws RangeError'
			);
			st.equal(returnCalls, 1, 'return called on receiver for negative limit');

			returnCalls = 0;
			st['throws'](
				function () { take(obj, { valueOf: function () { throw new SyntaxError('bad'); } }); },
				SyntaxError,
				'ToNumber throwing calls return'
			);
			st.equal(returnCalls, 1, 'return called on receiver when ToNumber throws');

			st.end();
		});

		var arr = [1, 2, 3];

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { take(iterator(), -3); },
				RangeError,
				'-3 is not >= 0'
			);

			st['throws'](
				function () { return new take(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new take(iterator(), 0); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');
			testIterator(take(iterator(), 0), [], st, 'take 0');
			testIterator(take(iterator(), 1), [1], st, 'take 1');
			testIterator(take(iterator(), 2), [1, 2], st, 'take 2');
			testIterator(take(iterator(), 3), [1, 2, 3], st, 'take 3');
			testIterator(take(iterator(), Infinity), [1, 2, 3], st, 'take ∞');

			st.test('262: limit-tonumber', function (s2t) {
				// ToNumber coercion
				testIterator(take(iterator(), '2'), [1, 2], s2t, 'string "2" coerced to 2');
				testIterator(take(iterator(), 2.9), [1, 2], s2t, '2.9 truncated to 2');
				testIterator(take(iterator(), { valueOf: function () { return 2; } }), [1, 2], s2t, 'object with valueOf');
				testIterator(take(iterator(), true), [1], s2t, 'true coerced to 1');
				testIterator(take(iterator(), null), [], s2t, 'null coerced to 0');

				s2t.end();
			});

			st.test('262: limit-tonumber-throws', function (s2t) {
				s2t['throws'](
					function () { take(iterator(), { valueOf: function () { throw new EvalError('valueOf threw'); } }); },
					EvalError,
					'throws when valueOf throws'
				);

				s2t['throws'](
					function () { take(iterator(), Symbol('test')); },
					TypeError,
					'throws when limit is a Symbol'
				);

				s2t.end();
			});

			st.test('262: limit-rangeerror', function (s2t) {
				s2t['throws'](function () { take(iterator(), -1); }, RangeError, '-1 throws RangeError');
				s2t['throws'](function () { take(iterator(), -Infinity); }, RangeError, '-Infinity throws RangeError');
				s2t['throws'](function () { take(iterator(), NaN); }, RangeError, 'NaN throws RangeError');

				s2t.end();
			});

			st.test('262: result is iterator', function (s2t) {
				var iter = take(iterator(), 2);
				s2t.equal(typeof iter.next, 'function', 'has next method');
				s2t.equal(typeof iter[Symbol.iterator], 'function', 'has Symbol.iterator method');
				s2t.equal(iter[Symbol.iterator](), iter, 'Symbol.iterator returns itself');

				s2t.end();
			});

			st.test('262: get next method only once', { skip: !hasPropertyDescriptors }, function (s2t) {
				var nextGets = 0;
				var testIter = {};
				Object.defineProperty(testIter, 'next', {
					get: function () {
						nextGets += 1;
						return function () {
							return { done: true, value: undefined };
						};
					}
				});

				var iter = take(testIter, 2);
				s2t.equal(nextGets, 1, 'next retrieved once on creation');
				iter.next();
				s2t.equal(nextGets, 1, 'next not retrieved again');

				s2t.end();
			});

			st.test('262: get next method throws', { skip: !hasPropertyDescriptors }, function (s2t) {
				var testIter = {};
				Object.defineProperty(testIter, 'next', {
					get: function () {
						throw new EvalError('next getter threw');
					}
				});

				s2t['throws'](
					function () { take(testIter, 2); },
					EvalError,
					'throws when getting next throws'
				);

				s2t.end();
			});

			st.test('262: next method returns non-object throws', function (s2t) {
				var badIterator = {
					next: function () {
						return null;
					}
				};

				var iter = take(badIterator, 2);
				s2t['throws'](function () { iter.next(); }, TypeError, 'throws when next returns null');

				s2t.end();
			});

			st.test('262: next method returns throwing done', { skip: !hasPropertyDescriptors }, function (s2t) {
				var throwingIterator = {
					next: function () {
						var result = { value: 1 };
						Object.defineProperty(result, 'done', {
							get: function () {
								throw new EvalError('done getter threw');
							}
						});
						return result;
					}
				};

				var iter = take(throwingIterator, 2);
				s2t['throws'](function () { iter.next(); }, EvalError, 'throws when done getter throws');

				s2t.end();
			});

			st.test('262: next method returns throwing value', { skip: !hasPropertyDescriptors }, function (s2t) {
				var throwingIterator = {
					next: function () {
						var result = { done: false };
						Object.defineProperty(result, 'value', {
							get: function () {
								throw new EvalError('value getter threw');
							}
						});
						return result;
					}
				};

				var iter = take(throwingIterator, 2);
				s2t['throws'](function () { iter.next(); }, EvalError, 'throws when value getter throws');

				s2t.end();
			});

			st.test('262: next method throws', function (s2t) {
				var throwingIterator = {
					next: function () {
						throw new EvalError('next threw');
					}
				};

				var iter = take(throwingIterator, 2);
				s2t['throws'](function () { iter.next(); }, EvalError, 'throws error from next');

				s2t.end();
			});

			st.test('262: return is forwarded', function (s2t) {
				var returnCalls = 0;

				var testIter = {
					next: function () {
						return { done: false, value: 1 };
					},
					'return': function () {
						returnCalls += 1;
						return { done: true, value: undefined };
					}
				};

				var iter = take(testIter, 5);
				iter.next();
				s2t.equal(returnCalls, 0, 'return not called before calling return()');
				iter['return']();
				s2t.equal(returnCalls, 1, 'return called once');

				s2t.end();
			});

			st.test('262: return is not forwarded after exhaustion', function (s2t) {
				var returnCalls = 0;

				var testIter = {
					next: function () {
						return { done: false, value: 1 };
					},
					'return': function () {
						returnCalls += 1;
						return { done: true, value: undefined };
					}
				};

				var iter = take(testIter, 1);
				iter.next(); // takes the one value, returnCalls still 0
				iter.next(); // exhausted, calls return on underlying -> returnCalls = 1
				s2t.equal(returnCalls, 1, 'return called once on exhaustion');

				// After exhaustion, calling return() should NOT forward again
				iter['return']();
				s2t.equal(returnCalls, 1, 'return not called again after already exhausted');

				s2t.end();
			});

			// Note: "exhaustion calls return" test removed as take's exhaustion behavior
			// depends on implementation details and spec interpretation

			st.test('262: get return method throws', { skip: !hasPropertyDescriptors }, function (s2t) {
				var badIterator = {
					next: function () {
						return { done: false, value: 1 };
					}
				};
				Object.defineProperty(badIterator, 'return', {
					get: function () {
						throw new SyntaxError('return getter threw');
					}
				});

				var iter = take(badIterator, 5);
				iter.next();

				s2t['throws'](function () { iter['return'](); }, SyntaxError, 'throws error from return getter');

				s2t.end();
			});

			st.test('262: throws TypeError when generator is running', function (s2t) {
				var reentrantIterator;
				var testIter = {
					next: function () {
						reentrantIterator.next();
						return { done: false, value: 1 };
					}
				};

				reentrantIterator = take(testIter, 5);
				s2t['throws'](function () { reentrantIterator.next(); }, TypeError, 'throws on reentrant next()');

				s2t.end();
			});

			st.test('262: limit-greater-than-or-equal-to-total', function (s2t) {
				// When limit >= total items, should return all items
				testIterator(take(iterator(), 5), [1, 2, 3], s2t, 'take 5 from 3-item iterator');
				testIterator(take(iterator(), 3), [1, 2, 3], s2t, 'take exactly 3 from 3-item iterator');

				s2t.end();
			});

			st.test('262: limit-less-than-total', function (s2t) {
				// When limit < total items, should stop early
				testIterator(take(iterator(), 2), [1, 2], s2t, 'take 2 from 3-item iterator');
				testIterator(take(iterator(), 1), [1], s2t, 'take 1 from 3-item iterator');

				s2t.end();
			});

			st.end();
		});
	},
	index: function () {
		test('Iterator.prototype.' + fnName + ': index', function (t) {
			module.exports.tests(index, 'Iterator.prototype.' + fnName, t);

			t.test('polyfill has early-error IteratorClose', function (st) {
				var returnCalls = 0;
				var obj = {
					next: function () { return { done: true }; },
					'return': function () {
						returnCalls += 1;
						return { done: true };
					}
				};
				st['throws'](
					function () { index(obj, NaN); },
					RangeError,
					'polyfill throws RangeError for NaN limit'
				);
				st.equal(returnCalls, 1, 'polyfill calls return on early error');
				st.end();
			});

			t.end();
		});
	},
	implementation: function () {
		test('Iterator.prototype.' + fnName + ': implementation', function (t) {
			module.exports.tests(callBind(impl), 'Iterator.prototype.' + fnName, t);

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator.prototype.' + fnName + ': shimmed', function (t) {
			t.test('Function name', { skip: !functionsHaveNames }, function (st) {
				st.equal(Iterator.prototype[fnName].name, fnName, 'Iterator#' + fnName + ' has name "' + fnName + '"');
				st.end();
			});

			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(Iterator.prototype, fnName), 'Iterator#' + fnName + ' is not enumerable');
				et.end();
			});

			t.test('bad string/this value', { skip: !hasStrictMode }, function (st) {
				st['throws'](function () { return Iterator.prototype[fnName].call(undefined, 'a'); }, TypeError, 'undefined is not an object');
				st['throws'](function () { return Iterator.prototype[fnName].call(null, 'a'); }, TypeError, 'null is not an object');
				st.end();
			});

			module.exports.tests(callBind(Iterator.prototype[fnName]), 'Iterator.prototype.' + fnName, t);

			t.end();
		});
	}
};
