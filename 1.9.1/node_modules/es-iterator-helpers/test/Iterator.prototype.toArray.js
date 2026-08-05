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

var index = require('../Iterator.prototype.toArray');
var impl = require('../Iterator.prototype.toArray/implementation');

var fnName = 'toArray';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (toArray, name, t) {
		t['throws'](
			function () { return new toArray(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(toArray(nonIterator)); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(toArray(badNext)); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new toArray(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');

			st.deepEqual(toArray(iterator()), [1, 2, 3], 'toArray');

			st.test('262: iterator-already-exhausted', function (s2t) {
				var exhaustedIter = {
					next: function () {
						return { done: true, value: undefined };
					}
				};

				var result = toArray(exhaustedIter);
				s2t.ok(Array.isArray(result), 'result is an array');
				s2t.deepEqual(result, [], 'empty array for exhausted iterator');

				s2t.end();
			});

			st.test('262: get-next-method-only-once', { skip: !hasPropertyDescriptors }, function (s2t) {
				var nextGets = 0;
				var callCount = 0;
				var testIter = {};
				Object.defineProperty(testIter, 'next', {
					get: function () {
						nextGets += 1;
						return function () {
							callCount += 1;
							if (callCount <= 2) {
								return { done: false, value: callCount };
							}
							return { done: true, value: undefined };
						};
					}
				});

				var result = toArray(testIter);
				s2t.equal(nextGets, 1, 'next retrieved once');
				s2t.deepEqual(result, [1, 2], 'result is correct');

				s2t.end();
			});

			st.test('262: get-next-method-throws', { skip: !hasPropertyDescriptors }, function (s2t) {
				var testIter = {};
				Object.defineProperty(testIter, 'next', {
					get: function () {
						throw new EvalError('next getter threw');
					}
				});

				s2t['throws'](
					function () { toArray(testIter); },
					EvalError,
					'throws when getting next throws'
				);

				s2t.end();
			});

			st.test('262: next-method-returns-non-object', function (s2t) {
				var badIterator = {
					next: function () {
						return null;
					}
				};

				s2t['throws'](
					function () { toArray(badIterator); },
					TypeError,
					'throws when next returns null'
				);

				var badIterator2 = {
					next: function () {
						return 42;
					}
				};

				s2t['throws'](
					function () { toArray(badIterator2); },
					TypeError,
					'throws when next returns a number'
				);

				s2t.end();
			});

			st.test('262: next-method-returns-throwing-done', { skip: !hasPropertyDescriptors }, function (s2t) {
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

				s2t['throws'](
					function () { toArray(throwingIterator); },
					EvalError,
					'throws when done getter throws'
				);

				s2t.end();
			});

			st.test('262: next-method-returns-throwing-value', { skip: !hasPropertyDescriptors }, function (s2t) {
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

				s2t['throws'](
					function () { toArray(throwingIterator); },
					EvalError,
					'throws when value getter throws'
				);

				s2t.end();
			});

			st.test('262: next-method-returns-throwing-value-done', { skip: !hasPropertyDescriptors }, function (s2t) {
				// Value property should not be accessed after done: true
				var valueAccessed = false;
				var doneIterator = {
					next: function () {
						var result = { done: true };
						Object.defineProperty(result, 'value', {
							get: function () {
								valueAccessed = true;
								throw new EvalError('value getter threw');
							}
						});
						return result;
					}
				};

				var result = toArray(doneIterator);
				s2t.deepEqual(result, [], 'toArray returns empty array');
				s2t.equal(valueAccessed, false, 'value not accessed when done is true');

				s2t.end();
			});

			st.test('262: next-method-throws', function (s2t) {
				var throwingIterator = {
					next: function () {
						throw new EvalError('next threw');
					}
				};

				s2t['throws'](
					function () { toArray(throwingIterator); },
					EvalError,
					'throws error from next'
				);

				s2t.end();
			});

			st.test('262: this-plain-iterator', function (s2t) {
				// toArray works with plain iterator objects not inheriting from Iterator.prototype
				var callCount = 0;
				var plainIterator = {
					next: function () {
						callCount += 1;
						if (callCount <= 3) {
							return { done: false, value: callCount };
						}
						return { done: true, value: undefined };
					}
				};

				var result = toArray(plainIterator);
				s2t.deepEqual(result, [1, 2, 3], 'toArray works with plain iterator');

				s2t.end();
			});

			st.end();
		});
	},
	index: function () {
		test('Iterator.prototype.' + fnName + ': index', function (t) {
			module.exports.tests(index, 'Iterator.prototype.' + fnName, t);

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
