'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var callBind = require('call-bind');
var functionsHaveNames = require('functions-have-names')();
var forEach = require('for-each');
var debug = require('object-inspect');
var v = require('es-value-fixtures');
var hasSymbols = require('has-symbols/shams')();
var mockProperty = require('mock-property');

var index = require('../Iterator.zip');
var impl = require('../Iterator.zip/implementation');
var from = require('../Iterator.from/polyfill')();

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (zip, name, t) {
		t['throws'](
			function () { return new zip(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor'
		);
		t['throws'](
			function () { return new zip({}); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor, with an argument'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { zip(nonIterator, []); },
				TypeError,
				debug(nonIterator) + ' is not an iterable Object'
			);
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			forEach(v.nonFunctions, function (nonFunction) {
				if (nonFunction != null) {
					var badIterable = {};
					badIterable[Symbol.iterator] = nonFunction;
					st['throws'](
						function () { zip([[], badIterable, []]).next(); },
						TypeError,
						debug(badIterable) + ' is not a function'
					);
				}
			});

			forEach(v.strings, function (string) {
				st['throws'](
					function () { zip([string]); },
					TypeError,
					'non-objects are not considered iterable'
				);
			});

			var arrayIt = zip([[1, 2, 3]]);
			st.equal(typeof arrayIt.next, 'function', 'has a `next` function');

			st.test('real iterators', { skip: !hasSymbols }, function (s2t) {
				var iter = [1, 2][Symbol.iterator]();
				testIterator(zip([iter, [3, 4]]), [[1, 3], [2, 4]], s2t, 'array iterator + array yields combined results');

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

				zip([from('')]);
				s2t.equal(observedType, 'string', 'string primitive -> primitive receiver in Symbol.iterator getter');
				zip([from(Object(''))]);
				s2t.equal(observedType, 'object', 'boxed string -> boxed string in Symbol.iterator getter');

				s2t.end();
			});

			st.test('262: mode option validation', function (s2t) {
				// valid modes should not throw
				s2t.doesNotThrow(function () { zip([[1], [2]]); }, 'undefined mode is valid');
				s2t.doesNotThrow(function () { zip([[1], [2]], { mode: undefined }); }, 'explicit undefined mode is valid');
				s2t.doesNotThrow(function () { zip([[1], [2]], { mode: 'shortest' }); }, '"shortest" mode is valid');
				s2t.doesNotThrow(function () { zip([[1], [2]], { mode: 'longest' }); }, '"longest" mode is valid');
				s2t.doesNotThrow(function () { zip([[1], [2]], { mode: 'strict' }); }, '"strict" mode is valid');

				// invalid modes should throw TypeError
				s2t['throws'](function () { zip([[1], [2]], { mode: null }); }, TypeError, 'null mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: false }); }, TypeError, 'false mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: '' }); }, TypeError, 'empty string mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: 'short' }); }, TypeError, '"short" mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: 'long' }); }, TypeError, '"long" mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: 'loose' }); }, TypeError, '"loose" mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: 0 }); }, TypeError, '0 mode throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: {} }); }, TypeError, 'object mode throws TypeError');

				// String wrapper should not be coerced
				s2t['throws'](function () { zip([[1], [2]], { mode: Object('shortest') }); }, TypeError, 'String wrapper mode throws TypeError');

				// objects with toString/valueOf should not be coerced
				s2t['throws'](function () { zip([[1], [2]], { mode: { toString: function () { return 'shortest'; } } }); }, TypeError, 'object with toString throws TypeError');
				s2t['throws'](function () { zip([[1], [2]], { mode: { valueOf: function () { return 'shortest'; } } }); }, TypeError, 'object with valueOf throws TypeError');

				s2t.end();
			});

			st.test('262: basic shortest mode', function (s2t) {
				// shortest mode (default) stops at minimum length
				testIterator(zip([[1, 2, 3], [4, 5]]), [[1, 4], [2, 5]], s2t, 'shortest mode stops at shorter iterator');
				testIterator(zip([[1, 2], [3, 4, 5]]), [[1, 3], [2, 4]], s2t, 'shortest mode stops at shorter first iterator');
				testIterator(zip([[1], [2], [3]]), [[1, 2, 3]], s2t, 'three iterators of length 1');
				testIterator(zip([[], [1, 2, 3]]), [], s2t, 'empty first iterator yields nothing');
				testIterator(zip([[1, 2, 3], []]), [], s2t, 'empty second iterator yields nothing');

				// explicit shortest mode
				testIterator(zip([[1, 2, 3], [4, 5]], { mode: 'shortest' }), [[1, 4], [2, 5]], s2t, 'explicit shortest mode');

				s2t.end();
			});

			st.test('262: basic longest mode', function (s2t) {
				// longest mode continues with undefined padding by default
				testIterator(zip([[1, 2, 3], [4, 5]], { mode: 'longest' }), [[1, 4], [2, 5], [3, undefined]], s2t, 'longest mode pads with undefined');
				testIterator(zip([[1, 2], [3, 4, 5]], { mode: 'longest' }), [[1, 3], [2, 4], [undefined, 5]], s2t, 'longest mode pads first iterator');
				testIterator(zip([[1], [2, 3], [4, 5, 6]], { mode: 'longest' }), [[1, 2, 4], [undefined, 3, 5], [undefined, undefined, 6]], s2t, 'longest mode with three iterators');

				s2t.end();
			});

			st.test('262: longest mode with padding iterable', function (s2t) {
				// padding iterable provides custom padding values per iterator slot
				// padding[0] = 'x' for first iterator, padding[1] = 'y' for second iterator
				testIterator(
					zip([[1, 2, 3], [4, 5]], { mode: 'longest', padding: ['x', 'y'] }),
					[[1, 4], [2, 5], [3, 'y']],
					s2t,
					'longest mode uses padding iterable values'
				);

				s2t.end();
			});

			st.test('262: longest mode padding iterable throws', function (s2t) {
				var throwingPadding = {};
				throwingPadding[Symbol.iterator] = function () {
					return {
						next: function () { throw new EvalError('padding next threw'); }
					};
				};

				var returnCalls = 0;
				var iter1 = {
					next: function () { return { done: false, value: 1 }; },
					'return': function () {
						returnCalls += 1;
						return { done: true };
					}
				};
				iter1[Symbol.iterator] = function () { return iter1; };

				s2t['throws'](
					function () { zip([iter1, [2]], { mode: 'longest', padding: throwingPadding }); },
					EvalError,
					'error from padding iterator propagates'
				);
				s2t.equal(returnCalls, 1, 'underlying iterators are closed when padding iterator throws');

				s2t.end();
			});

			st.test('262: longest mode padding iterable has fewer items', function (s2t) {
				// padding iterable has only 1 item but there are 3 iterators;
				// padding[0] = 'pad', padding[1] = undefined, padding[2] = undefined
				// third iterator (index 2) exhausts first, its padding is undefined
				testIterator(
					zip([[1, 2], [3, 4], [5]], { mode: 'longest', padding: ['pad'] }),
					[[1, 3, 5], [2, 4, undefined]],
					s2t,
					'longest mode with short padding: first slot uses padding value, rest fall back to undefined'
				);

				// padding iterable is empty
				testIterator(
					zip([[1, 2, 3], [4, 5]], { mode: 'longest', padding: [] }),
					[[1, 4], [2, 5], [3, undefined]],
					s2t,
					'longest mode with empty padding iterable falls back to undefined'
				);

				s2t.end();
			});

			st.test('262: longest mode padding iterator is closed when not exhausted', function (s2t) {
				var paddingReturnCalls = 0;
				var paddingIter = {
					next: function () { return { done: false, value: 'p' }; },
					'return': function () {
						paddingReturnCalls += 1;
						return { done: true };
					}
				};
				paddingIter[Symbol.iterator] = function () { return paddingIter; };

				// 2 iterables means 2 padding values needed, but paddingIter is infinite
				// so it should be closed after consuming 2 values
				var iter = zip([[1], [2]], { mode: 'longest', padding: paddingIter });
				s2t.equal(paddingReturnCalls, 1, 'padding iterator is closed when not exhausted');

				// verify the iterator still works
				testIterator(iter, [[1, 2]], s2t, 'longest mode with padding object produces correct results');

				s2t.end();
			});

			st.test('262: longest mode padding iterator close throws', function (s2t) {
				var paddingIter = {
					next: function () { return { done: false, value: 'p' }; },
					'return': function () {
						throw new EvalError('padding close threw');
					}
				};
				paddingIter[Symbol.iterator] = function () { return paddingIter; };

				var returnCalls = 0;
				var iter1 = {
					next: function () { return { done: false, value: 1 }; },
					'return': function () {
						returnCalls += 1;
						return { done: true };
					}
				};
				iter1[Symbol.iterator] = function () { return iter1; };

				s2t['throws'](
					function () { zip([iter1], { mode: 'longest', padding: paddingIter }); },
					EvalError,
					'error from closing padding iterator propagates'
				);
				s2t.equal(returnCalls, 1, 'underlying iterators are closed when padding close throws');

				s2t.end();
			});

			st.test('262: basic strict mode', function (s2t) {
				// strict mode succeeds when lengths match
				testIterator(zip([[1, 2], [3, 4]], { mode: 'strict' }), [[1, 3], [2, 4]], s2t, 'strict mode succeeds with equal lengths');
				testIterator(zip([[1], [2], [3]], { mode: 'strict' }), [[1, 2, 3]], s2t, 'strict mode with three iterators of length 1');

				// strict mode throws when lengths differ
				var strictIter1 = zip([[1, 2, 3], [4, 5]], { mode: 'strict' });
				strictIter1.next(); // [1, 4]
				strictIter1.next(); // [2, 5]
				s2t['throws'](function () { strictIter1.next(); }, TypeError, 'strict mode throws when first iterator has more');

				var strictIter2 = zip([[1, 2], [3, 4, 5]], { mode: 'strict' });
				strictIter2.next(); // [1, 3]
				strictIter2.next(); // [2, 4]
				s2t['throws'](function () { strictIter2.next(); }, TypeError, 'strict mode throws when second iterator has more');

				s2t.end();
			});

			st.test('262: strict mode throws when later iterator is not done', function (s2t) {
				// 3 iterators: first two done, third still has values
				var closeCalls = [];
				var iterA = [1][Symbol.iterator]();
				var iterB = [2][Symbol.iterator]();
				var iterC = {
					next: function () { return { done: false, value: 3 }; },
					'return': function () { closeCalls.push('c'); return { done: true }; }
				};
				iterC[Symbol.iterator] = function () { return iterC; };

				var strictIter = zip([iterA, iterB, iterC], { mode: 'strict' });
				strictIter.next(); // [1, 2, 3]
				s2t['throws'](
					function () { strictIter.next(); },
					TypeError,
					'strict mode throws when later iterator still has values'
				);
				s2t.ok(closeCalls.length > 0, 'remaining iterators are closed');

				s2t.end();
			});

			st.test('strict mode: errored iterator not double-closed', function (s2t) {
				var closeCounts = { a: 0, b: 0 };
				var iterA = {
					next: function () { return { done: true, value: undefined }; },
					'return': function () {
						closeCounts.a += 1;
						return { done: true };
					}
				};
				iterA[Symbol.iterator] = function () { return iterA; };
				var iterB = {
					next: function () { throw new EvalError('iterB.next threw'); },
					'return': function () {
						closeCounts.b += 1;
						return { done: true };
					}
				};
				iterB[Symbol.iterator] = function () { return iterB; };

				var strictZip = zip([iterA, iterB], { mode: 'strict' });
				// iterA.next() returns done:true, then in strict mode we check iterB
				// iterB.next() throws - iterB should be removed from openIters before closing
				s2t['throws'](
					function () { strictZip.next(); },
					EvalError,
					'strict mode propagates error from next()'
				);
				s2t.equal(closeCounts.b, 0, 'errored iterator is not closed again');

				s2t.end();
			});

			st.test('262: padding option validation', function (s2t) {
				// padding is only used in longest mode
				s2t.doesNotThrow(function () { zip([[1], [2]], { mode: 'shortest', padding: null }); }, 'invalid padding ignored in shortest mode');
				s2t.doesNotThrow(function () { zip([[1], [2]], { mode: 'strict', padding: null }); }, 'invalid padding ignored in strict mode');

				// invalid padding in longest mode
				s2t['throws'](function () { zip([[1], [2]], { mode: 'longest', padding: null }); }, TypeError, 'null padding throws in longest mode');
				s2t['throws'](function () { zip([[1], [2]], { mode: 'longest', padding: 'abc' }); }, TypeError, 'string padding throws in longest mode');
				s2t['throws'](function () { zip([[1], [2]], { mode: 'longest', padding: 123 }); }, TypeError, 'number padding throws in longest mode');
				s2t['throws'](function () { zip([[1], [2]], { mode: 'longest', padding: true }); }, TypeError, 'boolean padding throws in longest mode');

				s2t.end();
			});

			st.test('262: result is iterator', function (s2t) {
				var zipIter = zip([[1, 2], [3, 4]]);
				s2t.equal(typeof zipIter.next, 'function', 'has next method');
				s2t.equal(typeof zipIter[Symbol.iterator], 'function', 'has Symbol.iterator method');
				s2t.equal(zipIter[Symbol.iterator](), zipIter, 'Symbol.iterator returns itself');

				s2t.end();
			});

			st.test('262: return closes all underlying iterators', function (s2t) {
				var return1Calls = 0;
				var return2Calls = 0;

				var iter1 = {
					next: function () { return { done: false, value: 1 }; },
					'return': function () {
						return1Calls += 1;
						return { done: true, value: undefined };
					}
				};
				iter1[Symbol.iterator] = function () { return iter1; };

				var iter2 = {
					next: function () { return { done: false, value: 2 }; },
					'return': function () {
						return2Calls += 1;
						return { done: true, value: undefined };
					}
				};
				iter2[Symbol.iterator] = function () { return iter2; };

				var zipIter = zip([iter1, iter2]);
				zipIter.next();
				s2t.equal(return1Calls, 0, 'return not called before calling return()');
				s2t.equal(return2Calls, 0, 'return not called before calling return()');

				zipIter['return']();
				s2t.equal(return1Calls, 1, 'iter1.return called once');
				s2t.equal(return2Calls, 1, 'iter2.return called once');

				s2t.end();
			});

			st.test('262: next method throws closes other iterators', function (s2t) {
				var return2Calls = 0;

				var iter1 = {
					next: function () { throw new EvalError('iter1 next threw'); }
				};
				iter1[Symbol.iterator] = function () { return iter1; };

				var iter2 = {
					next: function () { return { done: false, value: 2 }; },
					'return': function () {
						return2Calls += 1;
						return { done: true, value: undefined };
					}
				};
				iter2[Symbol.iterator] = function () { return iter2; };

				var zipIter = zip([iter1, iter2]);
				s2t['throws'](function () { zipIter.next(); }, EvalError, 'throws error from iter1.next');
				s2t.equal(return2Calls, 1, 'iter2.return called when iter1.next throws');

				s2t.end();
			});

			st.end();
		});
	},
	index: function () {
		test('Iterator.zip: index', function (t) {
			module.exports.tests(index, 'Iterator.zip', t);

			t.end();
		});
	},
	implementation: function () {
		test('Iterator.zip: implementation', function (t) {
			module.exports.tests(impl, 'Iterator.zip', t);

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator.zip: shimmed', function (t) {
			t.test('Function name', { skip: !functionsHaveNames }, function (st) {
				st.equal(Iterator.zip.name, 'zip', 'Iterator.zip has name "zip"');
				st.end();
			});

			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(Iterator, 'zip'), 'Iterator.zip is not enumerable');
				et.end();
			});

			module.exports.tests(callBind(Iterator.zip, Iterator), 'Iterator.zip', t);

			t.end();
		});
	}
};
