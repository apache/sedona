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

var index = require('../Iterator.prototype.filter');
var impl = require('../Iterator.prototype.filter/implementation');

var fnName = 'filter';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (filter, name, t) {
		t['throws'](
			function () { return new filter(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(filter(nonIterator, function () {})); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(filter(badNext, function () {})); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		forEach(v.nonFunctions, function (nonFunction) {
			t['throws'](
				function () { filter({ next: function () {} }, nonFunction); },
				TypeError,
				debug(nonFunction) + ' is not a function'
			);
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
				function () { filter(obj, 'not a function'); },
				TypeError,
				'throws TypeError for non-callable predicate'
			);
			st.equal(returnCalls, 1, 'return called on receiver when IsCallable fails');
			st.end();
		});

		t.test('observable lookups', { skip: !hasPropertyDescriptors }, function (st) {
			var effects = [];

			var obj = {};
			Object.defineProperty(obj, 'next', {
				configurable: true,
				enumerable: true,
				get: function next() {
					effects.push('get next');
					return function () {
						return { done: true, value: undefined };
					};
				}
			});

			st['throws'](
				function () { filter(obj, null); },
				TypeError
			);

			st.deepEqual(effects, []);

			st.end();
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new filter(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new filter(iterator(), function () {}); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');
			testIterator(filter(iterator(), function () { return false; }), [], st, 'filter for always-false');
			testIterator(filter(iterator(), function () { return true; }), [1, 2, 3], st, 'filter for always-true');
			testIterator(filter(iterator(), function (x, i) { return x === 2 && i === 1; }), [2], st, 'filter returns value for matching value/index');

			st.end();
		});

		t.test('262: test/built-ins/Iterator/prototype/filter/predicate-args', function (st) {
			var g = function g() {
				var arr = ['a', 'b', 'c'];
				var i = 0;
				return {
					next: function () {
						try {
							return {
								value: arr[i],
								done: i >= arr.length
							};
						} finally {
							i += 1;
						}
					}
				};
			};
			var assertionCount = 0;
			var iter = filter(
				g(),
				function (value, count) {
					if (value === 'a') {
						st.equal(count, 0, 'first iteration');
					} else if (value === 'b') {
						st.equal(count, 1, 'second iteration');
					} else if (value === 'c') {
						st.equal(count, 2, 'third iteration');
					} else {
						st.fail('unexpected iteration');
					}
					assertionCount += 1;
					return true;
				}
			);

			st.equal(assertionCount, 0, 'prior to iteration');

			testIterator(iter, ['a', 'b', 'c'], st, 'iteration');

			st.equal(assertionCount, 3);

			st.end();
		});

		t.test('counter increments correctly with mixed matches', { skip: !hasSymbols }, function (st) {
			var counters = [];
			var iter = filter(
				[1, 2, 3, 4, 5][Symbol.iterator](),
				function (value, count) {
					counters.push(count);
					return value % 2 === 1; // only odd values match
				}
			);

			// 1 matches (count=0), 2 skipped (count=1), 3 matches (count=2)
			st.deepEqual(iter.next(), { value: 1, done: false }, 'first match');
			st.deepEqual(counters, [0], 'predicate called once so far');

			st.deepEqual(iter.next(), { value: 3, done: false }, 'second match');
			// predicate was called for 2 (count=1, no match) and 3 (count=2, match)
			st.deepEqual(counters, [0, 1, 2], 'counter increments for every call');

			st.deepEqual(iter.next(), { value: 5, done: false }, 'third match');
			st.deepEqual(counters, [0, 1, 2, 3, 4], 'counter continues incrementing');

			st.end();
		});

		t.test('counter not incremented after return while suspended', { skip: !hasSymbols }, function (st) {
			var counters = [];
			var returnCalled = false;
			var underlying = {
				next: function () {
					return { value: 1, done: false };
				},
				'return': function () {
					returnCalled = true;
					return { done: true };
				}
			};
			underlying[Symbol.iterator] = function () { return underlying; };

			var iter = filter(underlying, function (value, count) {
				counters.push(count);
				return true;
			});

			iter.next(); // predicate called with count=0, matches, Yield suspends
			st.deepEqual(counters, [0], 'predicate called with 0');

			// call return while suspended - counter should NOT have incremented yet
			iter['return']();
			st.equal(returnCalled, true, 'underlying return called');

			st.end();
		});

		t.test('262: test/built-ins/Iterator/prototype/filter/predicate-throws', function (st) {
			var returnCalls = 0;

			var iter = {
				next: function () {
					return {
						done: false,
						value: 1
					};
				},
				'return': function () {
					returnCalls += 1;
					return {};
				}
			};

			var callbackCalls = 0;
			var iterator = filter(iter, function () {
				callbackCalls += 1;
				throw new SyntaxError();
			});

			st['throws'](function () { iterator.next(); }, SyntaxError, 'next() throws');

			st.equal(callbackCalls, 1);
			st.equal(returnCalls, 1);

			st.end();
		});

		t.test('262: test/built-ins/Iterator/prototype/filter/predicate-throws-then-closing-iterator-also-throws', function (st) {
			var iter = {
				next: function next() {
					return {
						done: false,
						value: 1
					};
				},
				'return': function () {
					throw new EvalError();
				}
			};

			var iterator = filter(iter, function () {
				throw new SyntaxError();
			});

			st['throws'](
				function () { iterator.next(); },
				SyntaxError,
				'when the predicate and return() both throw, the predicate’s exception wins'
			);

			st.end();
		});

		t.test('262: test/built-ins/Iterator/prototype/filter/get-return-method-throws', { skip: !hasPropertyDescriptors }, function (st) {
			var badIterator = {
				next: function next() {
					return {
						done: false,
						value: 1
					};
				}
			};

			Object.defineProperty(badIterator, 'return', { get: function () { throw new SyntaxError(); } });

			var iter = filter(badIterator, function () { return true; });
			iter.next();

			st['throws'](
				function () { iter['return'](); },
				SyntaxError,
				'gets the `return` method, whose getter throws'
			);

			st.end();
		});

		t.test('262: test/built-ins/Iterator/prototype/drop/return-is-forwarded', function (st) {
			var returnCount = 0;

			var badIterator = {
				next: function next() {
					return {
						done: false,
						value: 1
					};
				},
				'return': function () {
					returnCount += 1;
					return {};
				}
			};

			var iter1 = filter(badIterator, function () { return false; });
			st.equal(returnCount, 0, 'iter1, before return()');
			iter1['return']();
			st.equal(returnCount, 1, 'iter1, after return()');

			st.end();
		});

		t.test('262: test/built-ins/Iterator/prototype/drop/return-is-not-forwarded-after-exhaustion', { skip: !hasPropertyDescriptors }, function (st) {
			var makeBadIterator = function makeBadIterator() {
				return {
					next: function next() {
						return {
							done: true,
							value: undefined
						};
					},
					'return': function () {
						throw new SyntaxError();
					}
				};
			};

			var iter1 = filter(makeBadIterator(), function () { return true; });
			st['throws'](
				function () { iter1['return'](); },
				SyntaxError,
				'iter1, return() throws'
			);
			iter1.next();
			iter1['return']();

			// 3 filters (i wish i had pipeline)
			var iter2 = filter(
				filter(
					filter(
						makeBadIterator(),
						function () { return true; }
					),
					function () { return true; }
				),
				function () { return true; }
			);
			st['throws']( // TODO
				function () { iter2['return'](); },
				SyntaxError,
				'iter2, return() throws'
			);
			iter2.next();
			iter2['return']();

			st.end();
		});

		t.test('262: predicate-this', { skip: !hasSymbols }, function (st) {
			var thisValue;
			var iter = filter(
				[1][Symbol.iterator](),
				function () {
					thisValue = this;
					return true;
				}
			);
			iter.next();
			st.equal(thisValue, undefined, 'predicate this is undefined');

			st.end();
		});

		t.test('262: predicate-returns-non-boolean', { skip: !hasSymbols }, function (st) {
			// truthy non-boolean values should pass
			testIterator(
				filter([1, 2, 3][Symbol.iterator](), function () { return 1; }),
				[1, 2, 3],
				st,
				'truthy number passes'
			);
			testIterator(
				filter([1, 2, 3][Symbol.iterator](), function () { return 'yes'; }),
				[1, 2, 3],
				st,
				'truthy string passes'
			);
			testIterator(
				filter([1, 2, 3][Symbol.iterator](), function () { return {}; }),
				[1, 2, 3],
				st,
				'truthy object passes'
			);

			// falsy non-boolean values should fail
			testIterator(
				filter([1, 2, 3][Symbol.iterator](), function () { return 0; }),
				[],
				st,
				'falsy number fails'
			);
			testIterator(
				filter([1, 2, 3][Symbol.iterator](), function () { return ''; }),
				[],
				st,
				'falsy string fails'
			);
			testIterator(
				filter([1, 2, 3][Symbol.iterator](), function () { return null; }),
				[],
				st,
				'null fails'
			);

			st.end();
		});

		t.test('262: result is iterator', { skip: !hasSymbols }, function (st) {
			var iterator = filter([1, 2][Symbol.iterator](), function () { return true; });
			st.equal(typeof iterator.next, 'function', 'has next method');
			st.equal(typeof iterator[Symbol.iterator], 'function', 'has Symbol.iterator method');
			st.equal(iterator[Symbol.iterator](), iterator, 'Symbol.iterator returns itself');

			st.end();
		});

		t.test('262: get next method only once', { skip: !hasPropertyDescriptors }, function (st) {
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

			var iter = filter(testIter, function () { return true; });
			st.equal(nextGets, 1, 'next retrieved once on creation');
			iter.next();
			st.equal(nextGets, 1, 'next not retrieved again');

			st.end();
		});

		t.test('262: get next method throws', { skip: !hasPropertyDescriptors }, function (st) {
			var testIter = {};
			Object.defineProperty(testIter, 'next', {
				get: function () {
					throw new EvalError('next getter threw');
				}
			});

			st['throws'](
				function () { filter(testIter, function () { return true; }); },
				EvalError,
				'throws when getting next throws'
			);

			st.end();
		});

		t.test('262: next method returns non-object throws', function (st) {
			var badIterator = {
				next: function () {
					return null;
				}
			};

			var iter = filter(badIterator, function () { return true; });
			st['throws'](function () { iter.next(); }, TypeError, 'throws when next returns null');

			st.end();
		});

		t.test('262: next method returns throwing done', { skip: !hasPropertyDescriptors }, function (st) {
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

			var iter = filter(throwingIterator, function () { return true; });
			st['throws'](function () { iter.next(); }, EvalError, 'throws when done getter throws');

			st.end();
		});

		t.test('262: next method returns throwing value', { skip: !hasPropertyDescriptors }, function (st) {
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

			var iter = filter(throwingIterator, function () { return true; });
			st['throws'](function () { iter.next(); }, EvalError, 'throws when value getter throws');

			st.end();
		});

		t.test('262: next method throws', function (st) {
			var throwingIterator = {
				next: function () {
					throw new EvalError('next threw');
				}
			};

			var iter = filter(throwingIterator, function () { return true; });
			st['throws'](function () { iter.next(); }, EvalError, 'throws error from next');

			st.end();
		});

		t.test('262: iterator already exhausted', function (st) {
			var testIter = {
				next: function () {
					return { done: true, value: undefined };
				}
			};

			var predicateCalls = 0;
			var iter = filter(testIter, function () {
				predicateCalls += 1;
				return true;
			});

			var result = iter.next();
			st.equal(result.done, true, 'done is true');
			st.equal(result.value, undefined, 'value is undefined');
			st.equal(predicateCalls, 0, 'predicate not called for exhausted iterator');

			st.end();
		});

		t.test('262: throws TypeError when generator is running', function (st) {
			var reentrantIterator;
			var testIter = {
				next: function () {
					reentrantIterator.next();
					return { done: false, value: 1 };
				}
			};

			reentrantIterator = filter(testIter, function () { return true; });
			st['throws'](function () { reentrantIterator.next(); }, TypeError, 'throws on reentrant next()');

			st.end();
		});

		t.test('262: exhaustion does not call return', function (st) {
			var returnCalls = 0;

			var testIter = {
				next: function () {
					return { done: true, value: undefined };
				},
				'return': function () {
					returnCalls += 1;
					return { done: true, value: undefined };
				}
			};

			var iter = filter(testIter, function () { return true; });
			iter.next();
			st.equal(returnCalls, 0, 'return not called on exhaustion');

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
					function () { index(obj, null); },
					TypeError,
					'polyfill throws TypeError for non-callable argument'
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
