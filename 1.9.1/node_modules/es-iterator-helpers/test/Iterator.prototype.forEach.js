'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var callBind = require('call-bind');
var functionsHaveNames = require('functions-have-names')();
var hasStrictMode = require('has-strict-mode')();
var forEachNormal = require('for-each');
var debug = require('object-inspect');
var v = require('es-value-fixtures');
var hasSymbols = require('has-symbols/shams')();
var iterate = require('iterate-iterator');

var index = require('../Iterator.prototype.forEach');
var impl = require('../Iterator.prototype.forEach/implementation');

var fnName = 'forEach';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (forEach, name, t) {
		t['throws'](
			function () { return new forEach(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` is not a constructor'
		);

		forEachNormal(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(forEach(nonIterator, function () {})); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(forEach(badNext, function () {})); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		forEachNormal(v.nonFunctions, function (nonFunction) {
			t['throws'](
				function () { forEach({ next: function () {} }, nonFunction); },
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
				function () { forEach(obj, 'not a function'); },
				TypeError,
				'throws TypeError for non-callable fn'
			);
			st.equal(returnCalls, 1, 'return called on receiver when IsCallable fails');
			st.end();
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new forEach(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new forEach(iterator(), function () {}); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');

			var results = [];
			var ret = forEach(
				iterator(),
				function (x, i) {

					results.push({ value: x, count: i, 'this': this, args: arguments.length });
				}
			);

			st.equal(ret, undefined, 'returns undefined');

			st.deepEqual(
				results,
				[
					{ value: 1, count: 0, 'this': undefined, args: 2 },
					{ value: 2, count: 1, 'this': undefined, args: 2 },
					{ value: 3, count: 2, 'this': undefined, args: 2 }
				],
				'forEach callback receives the expected values'
			);

			st.test('test262: test/built-ins/Iterator/prototype/forEach/fn-args', function (s2t) {
				var assertionCount = 0;
				var result = forEach(
					['a', 'b', 'c'][Symbol.iterator](),
					function (value, count) {
						if (value === 'a') {
							s2t.equal(count, 0);
						} else if (value === 'b') {
							s2t.equal(count, 1);
						} else if (value === 'c') {
							s2t.equal(count, 2);
						} else {
							s2t.fail('unexpected value');
						}
						assertionCount += 1;
					}
				);
				s2t.equal(result, undefined, 'forEach returns undefined');
				s2t.equal(assertionCount, 3, 'callback called three times');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/forEach/fn-this', function (s2t) {
				var expectedThis = (function () { return this; }());
				var assertionCount = 0;
				var result = forEach(
					[0][Symbol.iterator](),
					function () {
						s2t.equal(this, expectedThis, 'fn this is undefined');
						assertionCount += 1;
					}
				);
				s2t.equal(result, undefined);
				s2t.equal(assertionCount, 1);

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/forEach/fn-called-for-each-yielded-value', function (s2t) {
				var values = [];
				forEach(
					[1, 2, 3, 4, 5][Symbol.iterator](),
					function (value) { values.push(value); }
				);
				s2t.deepEqual(values, [1, 2, 3, 4, 5], 'callback called for each value');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/forEach/fn-throws', function (s2t) {
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
						forEach(testIter, function () {
							callbackCount += 1;
							throw new SyntaxError('callback threw');
						});
					},
					SyntaxError
				);
				s2t.equal(callbackCount, 1, 'callback called once');
				s2t.equal(returnCount, 1, 'iterator closed when callback throws');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/forEach/iterator-already-exhausted', function (s2t) {
				var iter = [][Symbol.iterator]();
				var callbackCount = 0;
				var result = forEach(iter, function () {
					callbackCount += 1;
				});
				s2t.equal(result, undefined, 'forEach returns undefined for empty iterator');
				s2t.equal(callbackCount, 0, 'callback not called');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/forEach/result-is-undefined', function (s2t) {
				var result = forEach(
					[1, 2, 3][Symbol.iterator](),
					function () {}
				);
				s2t.equal(result, undefined, 'forEach returns undefined');

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
