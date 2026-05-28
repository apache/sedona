'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var callBind = require('call-bind');
var functionsHaveNames = require('functions-have-names')();
var hasProto = require('has-proto')();
var forEach = require('for-each');
var debug = require('object-inspect');
var v = require('es-value-fixtures');
var hasSymbols = require('has-symbols/shams')();
var hasPropertyDescriptors = require('has-property-descriptors')();
var mockProperty = require('mock-property');

var index = require('../Iterator.from');
var impl = require('../Iterator.from/implementation');

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

var $Iterator = require('../Iterator/implementation');
var iterProto = require('iterator.prototype');

var getCodePoints = function getCodePoints(str) {
	var chars = [];
	for (var i = 0; i < str.length; i++) {
		var c1 = str.charCodeAt(i);
		if (c1 >= 0xD800 && c1 < 0xDC00 && i + 1 < str.length) {
			var c2 = str.charCodeAt(i + 1);
			if (c2 >= 0xDC00 && c2 < 0xE000) {
				chars.push(str.charAt(i) + str.charAt(i + 1));
				i += 1;
				continue; // eslint-disable-line no-continue, no-restricted-syntax
			}
		}
		chars.push(str.charAt(i));
	}
	return chars;
};

module.exports = {
	tests: function (from, name, t) {
		t['throws'](
			function () { return new from(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor'
		);
		t['throws'](
			function () { return new from({}); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor, with an argument'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			if (typeof nonIterator !== 'string') {
				t['throws'](
					function () { from(nonIterator).next(); },
					TypeError,
					debug(nonIterator) + ' is not an iterable Object'
				);
			}
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			forEach(v.nonFunctions, function (nonFunction) {
				var badIterable = {};
				badIterable[Symbol.iterator] = nonFunction;
				st['throws'](
					function () { from(badIterable).next(); },
					TypeError,
					debug(badIterable) + ' is not a function'
				);
			});

			// st['throws'](
			// 	function () { return new from([]); }, // eslint-disable-line new-cap
			// 	RangeError,
			// 	'`' + name + '` iterator is not a constructor'
			// );

			forEach(v.strings, function (string) {
				var stringIt = from(string);
				testIterator(stringIt, getCodePoints(string), st, 'string iterator: ' + debug(string));
			});

			var arrayIt = from([1, 2, 3]);
			st.equal(typeof arrayIt.next, 'function', 'has a `next` function');

			st.test('__proto__ is Iterator.prototype', { skip: !hasProto }, function (s2t) {
				var fakeIterator = {
					__proto__: iterProto,
					next: function () {}
				};
				s2t.ok(fakeIterator instanceof $Iterator, 'is an instanceof Iterator');
				s2t.equal(typeof fakeIterator.next, 'function', 'fake iterator `.next` is a function');
				s2t.equal(from(fakeIterator), fakeIterator, 'returns input when it is an instanceof Iterator');

				s2t.end();
			});

			st.test('real iterators', { skip: !hasSymbols }, function (s2t) {
				var iter = [][Symbol.iterator]();
				// eslint-disable-next-line no-proto
				var arrayIterHasIterProto = hasProto && iter.__proto__.__proto__ !== Object.prototype;
				s2t.equal(
					from(iter),
					iter,
					'array iterator becomes itself',
					{ skip: !arrayIterHasIterProto && 'node 0.12 - 3 do not have Iterator.prototype in the proto chains' }
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

				from('');
				s2t.equal(observedType, 'string', 'string primitive -> primitive receiver in Symbol.iterator getter');
				from(Object(''));
				s2t.equal(observedType, 'object', 'boxed string -> boxed string in Symbol.iterator getter');

				s2t.end();
			});

			st.test('262: get-next-method-only-once', { skip: !hasPropertyDescriptors }, function (s2t) {
				var nextGets = 0;
				var testIter = {
					next: function () {
						return { done: true, value: undefined };
					}
				};
				Object.defineProperty(testIter, 'next', {
					get: function () {
						nextGets += 1;
						return function () {
							return { done: true, value: undefined };
						};
					}
				});

				var iter = from(testIter);
				s2t.equal(nextGets, 1, 'next retrieved once on creation');
				iter.next();
				s2t.equal(nextGets, 1, 'next not retrieved again on next()');
				iter.next();
				s2t.equal(nextGets, 1, 'next still not retrieved again');

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
					function () { from(testIter); },
					EvalError,
					'throws when getting next throws'
				);

				s2t.end();
			});

			st.test('262: iterable-to-iterator-fallback', function (s2t) {
				// When Symbol.iterator is null/undefined, treat object as iterator
				var iteratorObj = {
					next: function () {
						return { done: true, value: undefined };
					}
				};
				iteratorObj[Symbol.iterator] = null;
				var iter1 = from(iteratorObj);
				s2t.equal(typeof iter1.next, 'function', 'iterator with null Symbol.iterator is treated as iterator');

				var iteratorObj2 = {
					next: function () {
						return { done: true, value: undefined };
					}
				};
				iteratorObj2[Symbol.iterator] = undefined;
				var iter2 = from(iteratorObj2);
				s2t.equal(typeof iter2.next, 'function', 'iterator with undefined Symbol.iterator is treated as iterator');

				s2t.end();
			});

			st.test('262: return-is-forwarded', function (s2t) {
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

				var iter = from(testIter);
				iter.next();
				s2t.equal(returnCalls, 0, 'return not called before calling return()');
				iter['return']();
				s2t.equal(returnCalls, 1, 'return called once after return()');

				s2t.end();
			});

			st.test('262: return-method-returns-iterator-result', function (s2t) {
				// When base iterator has no return method, wrapper's return() returns { done: true, value: undefined }
				var testIter = {
					next: function () {
						return { done: false, value: 1 };
					}
				};

				var iter = from(testIter);
				iter.next();
				var result = iter['return']();
				s2t.equal(result.done, true, 'done is true');
				s2t.equal(result.value, undefined, 'value is undefined');

				s2t.end();
			});

			st.test('262: get-return-method-throws', { skip: !hasPropertyDescriptors }, function (s2t) {
				var testIter = {
					next: function () {
						return { done: false, value: 1 };
					}
				};
				Object.defineProperty(testIter, 'return', {
					get: function () {
						throw new SyntaxError('return getter threw');
					}
				});

				var iter = from(testIter);
				iter.next();
				s2t['throws'](
					function () { iter['return'](); },
					SyntaxError,
					'throws when getting return throws'
				);

				s2t.end();
			});

			st.test('262: supports-iterator', function (s2t) {
				// Non-iterable iterator objects (no Symbol.iterator, but has next)
				var plainIterator = {
					next: function () {
						return { done: true, value: 42 };
					}
				};

				var iter = from(plainIterator);
				s2t.equal(typeof iter.next, 'function', 'wrapped iterator has next method');
				var result = iter.next();
				s2t.equal(result.done, true, 'done is true');
				s2t.equal(result.value, 42, 'value is passed through');

				s2t.end();
			});

			st.test('262: supports-iterable', function (s2t) {
				// Iterable objects work
				var arr = [1, 2, 3];
				testIterator(from(arr), [1, 2, 3], s2t, 'array iterable');

				s2t.end();
			});

			st.end();
		});
	},
	index: function () {
		test('Iterator.from: index', function (t) {
			module.exports.tests(index, 'Iterator.from', t);

			t.end();
		});
	},
	implementation: function () {
		test('Iterator.from: implementation', function (t) {
			module.exports.tests(impl, 'Iterator.from', t);

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator.from: shimmed', function (t) {
			t.test('Function name', { skip: !functionsHaveNames }, function (st) {
				st.equal(Iterator.from.name, 'from', 'Iterator.from has name "from"');
				st.end();
			});

			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(Iterator, 'from'), 'Iterator.from is not enumerable');
				et.end();
			});

			module.exports.tests(callBind(Iterator.from, Iterator), 'Iterator.from', t);

			t.end();
		});
	}
};
