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

var index = require('../Iterator.prototype.find');
var impl = require('../Iterator.prototype.find/implementation');

var fnName = 'find';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (find, name, t) {
		t['throws'](
			function () { return new find(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(find(nonIterator, function () {})); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(find(badNext, function () {})); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		forEach(v.nonFunctions, function (nonFunction) {
			t['throws'](
				function () { find({ next: function () {} }, nonFunction); },
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
				function () { find(obj, 'not a function'); },
				TypeError,
				'throws TypeError for non-callable predicate'
			);
			st.equal(returnCalls, 1, 'return called on receiver when IsCallable fails');
			st.end();
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new find(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new find(iterator(), function () {}); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');
			st.equal(find(iterator(), function () { return false; }), undefined, 'find for always-false');
			st.equal(find(iterator(), function () { return true; }), 1, 'find for always-true');
			st.equal(find(iterator(), function (x, i) { return x === 2 && i === 1; }), 2, 'find returns value for matching value/index');

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-args', function (s2t) {
				var assertionCount = 0;
				var result = find(
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
						return false;
					}
				);
				s2t.equal(result, undefined, 'find returns undefined when none match');
				s2t.equal(assertionCount, 3, 'predicate called three times');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-this', function (s2t) {
				var expectedThis = (function () { return this; }());
				var assertionCount = 0;
				var result = find(
					[0][Symbol.iterator](),
					function () {
						s2t.equal(this, expectedThis, 'predicate this is undefined');
						assertionCount += 1;
						return true;
					}
				);
				s2t.equal(result, 0);
				s2t.equal(assertionCount, 1);

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-returns-truthy', function (s2t) {
				var callCount = 0;
				var returnCalled = false;
				var values = [0, 1, 2];
				var idx = 0;
				var iter = {
					next: function () {
						if (idx < values.length) {
							var val = values[idx];
							idx += 1;
							return { done: false, value: val };
						}
						return { done: true, value: undefined };
					},
					'return': function () {
						returnCalled = true;
						return { done: true, value: undefined };
					}
				};
				var result = find(
					iter,
					function () {
						callCount += 1;
						return true;
					}
				);
				s2t.equal(result, 0, 'find returns first value when predicate returns truthy');
				s2t.equal(callCount, 1, 'predicate called once');
				s2t.equal(returnCalled, true, 'iterator is closed');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-returns-falsey', function (s2t) {
				var result = find(
					[0, 1, 2, 3, 4][Symbol.iterator](),
					function () { return false; }
				);
				s2t.equal(result, undefined, 'find returns undefined when predicate always returns falsey');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-returns-falsey-then-truthy', function (s2t) {
				var callCount = 0;
				var returnCalled = false;
				var values = [0, 1, 2, 3, 4];
				var idx = 0;
				var iter = {
					next: function () {
						if (idx < values.length) {
							var val = values[idx];
							idx += 1;
							return { done: false, value: val };
						}
						return { done: true, value: undefined };
					},
					'return': function () {
						returnCalled = true;
						return { done: true, value: undefined };
					}
				};
				var result = find(
					iter,
					function (value) {
						callCount += 1;
						return value >= 3;
					}
				);
				s2t.equal(result, 3, 'find returns matching value');
				s2t.equal(callCount, 4, 'predicate called until truthy returned');
				s2t.equal(returnCalled, true, 'iterator is closed');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-returns-non-boolean', function (s2t) {
				// truthy non-boolean values find the first element
				s2t.equal(find([1, 2][Symbol.iterator](), function () { return 1; }), 1, 'truthy number');
				s2t.equal(find([1, 2][Symbol.iterator](), function () { return 'yes'; }), 1, 'truthy string');
				s2t.equal(find([1, 2][Symbol.iterator](), function () { return {}; }), 1, 'truthy object');

				// falsy non-boolean values return undefined
				s2t.equal(find([1][Symbol.iterator](), function () { return 0; }), undefined, 'falsy number');
				s2t.equal(find([1][Symbol.iterator](), function () { return ''; }), undefined, 'falsy string');
				s2t.equal(find([1][Symbol.iterator](), function () { return null; }), undefined, 'null');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/predicate-throws', function (s2t) {
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
						find(testIter, function () {
							callbackCount += 1;
							throw new SyntaxError('predicate threw');
						});
					},
					SyntaxError
				);
				s2t.equal(callbackCount, 1, 'predicate called once');
				s2t.equal(returnCount, 1, 'iterator closed when predicate throws');

				s2t.end();
			});

			st.test('test262: test/built-ins/Iterator/prototype/find/iterator-already-exhausted', function (s2t) {
				var iter = [][Symbol.iterator]();
				var callbackCount = 0;
				var result = find(iter, function () {
					callbackCount += 1;
					return true;
				});
				s2t.equal(result, undefined, 'find returns undefined for empty iterator');
				s2t.equal(callbackCount, 0, 'predicate not called');

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
