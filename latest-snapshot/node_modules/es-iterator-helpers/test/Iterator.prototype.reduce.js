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
var iterate = require('iterate-iterator');

var index = require('../Iterator.prototype.reduce');
var impl = require('../Iterator.prototype.reduce/implementation');

var fnName = 'reduce';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (reduce, name, t) {
		t['throws'](
			function () { return new reduce(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(reduce(nonIterator, function () {})); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(reduce(badNext, function () {})); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		forEach(v.nonFunctions, function (nonFunction) {
			t['throws'](
				function () { reduce({ next: function () {} }, nonFunction); },
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
				function () { reduce(obj, 'not a function'); },
				TypeError,
				'throws TypeError for non-callable reducer'
			);
			st.equal(returnCalls, 1, 'return called on receiver when IsCallable fails');
			st.end();
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new reduce(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new reduce(iterator(), function () {}); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');

			var results = [];
			var ret = reduce(
				iterator(),
				function (acc, x, i) {

					results.push({ acc: acc, value: x, count: i, 'this': this, args: arguments.length });
					return acc + x;
				}
			);
			st.equal(ret, 6, 'returns sum of all numbers');
			st.deepEqual(
				results,
				[
					{ acc: 1, value: 2, count: 1, 'this': undefined, args: 3 },
					{ acc: 3, value: 3, count: 2, 'this': undefined, args: 3 }
				],
				'reduce callback receives the expected values without initialValue'
			);

			var results2 = [];
			var ret2 = reduce(
				iterator(),
				function (acc, x, i) {

					results2.push({ acc: acc, value: x, count: i, 'this': this, args: arguments.length });
					return acc + x;
				},
				10
			);
			st.equal(ret2, 16, 'returns sum of all numbers plus initialValue');
			st.deepEqual(
				results2,
				[
					{ acc: 10, value: 1, count: 0, 'this': undefined, args: 3 },
					{ acc: 11, value: 2, count: 1, 'this': undefined, args: 3 },
					{ acc: 13, value: 3, count: 2, 'this': undefined, args: 3 }
				],
				'reduce callback receives the expected values with initialValue'
			);

			st.test('test262: test/built-ins/Iterator/prototype/reduce/reducer-args-initial-value', function (s2t) {
				var assertionCount = 0;
				var result = reduce(
					['a', 'b', 'c'][Symbol.iterator](),
					function (memo, value, count) {
						if (value === 'a') {
							s2t.equal(memo, 'start', 'memo is initial value for first call');
							s2t.equal(count, 0);
						} else if (value === 'b') {
							s2t.equal(memo, 'a', 'memo is previous return');
							s2t.equal(count, 1);
						} else if (value === 'c') {
							s2t.equal(memo, 'b', 'memo is previous return');
							s2t.equal(count, 2);
						} else {
							s2t.fail('unexpected value');
						}
						assertionCount += 1;
						return value;
					},
					'start'
				);
				s2t.equal(result, 'c', 'reduce returns last value');
				s2t.equal(assertionCount, 3, 'reducer called three times');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/reducer-args-no-initial-value', function (s2t) {
				var assertionCount = 0;
				var result = reduce(
					['a', 'b', 'c'][Symbol.iterator](),
					function (memo, value, count) {
						if (value === 'b') {
							s2t.equal(memo, 'a', 'memo is first value when no initial');
							s2t.equal(count, 1);
						} else if (value === 'c') {
							s2t.equal(memo, 'b', 'memo is previous return');
							s2t.equal(count, 2);
						} else {
							s2t.fail('unexpected value');
						}
						assertionCount += 1;
						return value;
					}
				);
				s2t.equal(result, 'c', 'reduce returns last value');
				s2t.equal(assertionCount, 2, 'reducer called twice');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/reducer-this', function (s2t) {
				var expectedThis = (function () { return this; }());
				var assertionCount = 0;
				var result = reduce(
					[0, 1][Symbol.iterator](),
					function () {
						s2t.equal(this, expectedThis, 'reducer this is undefined');
						assertionCount += 1;
						return 0;
					}
				);
				s2t.equal(result, 0);
				s2t.equal(assertionCount, 1, 'reducer called once');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/reducer-throws', function (s2t) {
				var returnCount = 0;
				var testIter = {
					next: function () {
						return { done: false, value: 1 };
					},
					'return': function () {
						returnCount += 1;
						return { done: true };
					}
				};

				var callbackCount = 0;
				s2t['throws'](
					function () {
						reduce(testIter, function () {
							callbackCount += 1;
							throw new SyntaxError('reducer threw');
						}, 0);
					},
					SyntaxError
				);
				s2t.equal(callbackCount, 1, 'reducer called once');
				s2t.equal(returnCount, 1, 'iterator closed when reducer throws');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/iterator-already-exhausted-initial-value', function (s2t) {
				var iter = [][Symbol.iterator]();
				var callbackCount = 0;
				var result = reduce(iter, function () {
					callbackCount += 1;
					return 'called';
				}, 'initial');
				s2t.equal(result, 'initial', 'reduce returns initial value for empty iterator');
				s2t.equal(callbackCount, 0, 'reducer not called');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/iterator-already-exhausted-no-initial-value', function (s2t) {
				var iter = [][Symbol.iterator]();
				s2t['throws'](
					function () {
						reduce(iter, function () {});
					},
					TypeError,
					'reduce throws TypeError for empty iterator without initial value'
				);

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/iterator-yields-once-initial-value', function (s2t) {
				var callbackCount = 0;
				var result = reduce(
					[42][Symbol.iterator](),
					function (memo, value) {
						callbackCount += 1;
						return memo + value;
					},
					10
				);
				s2t.equal(result, 52, 'reduce returns sum');
				s2t.equal(callbackCount, 1, 'reducer called once');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/iterator-yields-once-no-initial-value', function (s2t) {
				var callbackCount = 0;
				var result = reduce(
					[42][Symbol.iterator](),
					function () {
						callbackCount += 1;
						return 'called';
					}
				);
				s2t.equal(result, 42, 'reduce returns sole value');
				s2t.equal(callbackCount, 0, 'reducer not called');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/reduce/reducer-memo-can-be-any-type', function (s2t) {
				// test various memo types
				s2t.equal(reduce([1][Symbol.iterator](), function (m) { return m; }, null), null, 'null memo');
				s2t.equal(reduce([1][Symbol.iterator](), function (m) { return m; }, undefined), undefined, 'undefined memo');
				s2t.equal(reduce([1][Symbol.iterator](), function (m) { return m; }, 0), 0, 'number memo');
				s2t.equal(reduce([1][Symbol.iterator](), function (m) { return m; }, ''), '', 'string memo');
				s2t.equal(reduce([1][Symbol.iterator](), function (m) { return m; }, false), false, 'boolean memo');
				var obj = {};
				s2t.equal(reduce([1][Symbol.iterator](), function (m) { return m; }, obj), obj, 'object memo');

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
