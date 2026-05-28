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
var generators = require('make-generator-function')();
var iterate = require('iterate-iterator');

var index = require('../Iterator.prototype.map');
var impl = require('../Iterator.prototype.map/implementation');

var fnName = 'map';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (map, name, t) {
		t['throws'](
			function () { return new map(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` itself is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(map(nonIterator, function () {})); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(map(badNext, function () {})); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		forEach(v.nonFunctions, function (nonFunction) {
			t['throws'](
				function () { map({ next: function () {} }, nonFunction); },
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
				function () { map(obj, 'not a function'); },
				TypeError,
				'throws TypeError for non-callable mapper'
			);
			st.equal(returnCalls, 1, 'return called on receiver when IsCallable fails');
			st.end();
		});

		var sentinel = {};
		var done = false;
		var fakeIterator = {
			next: function () {
				try {
					return {
						done: !!done,
						value: sentinel
					};
				} finally {
					done = done === false ? null : true;
				}
			}
		};
		var result = {};
		testIterator(
			map(fakeIterator, function (x, i) {
				result.value = x;
				result.counter = i;
				result.receiver = this;
				result.args = arguments.length;
				return fakeIterator;
			}),
			[fakeIterator, fakeIterator],
			t,
			'fake iterator, mapped, runs as expected'
		);
		t.deepEqual(
			result,
			{ value: sentinel, counter: 1, receiver: undefined, args: 2 },
			'callback is called with the correct arguments'
		);

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new map(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new map(iterator(), function () {}); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');
			testIterator(map(iterator(), function (x) { return x; }), [1, 2, 3], st, 'identity mapper');
			testIterator(map(iterator(), function (x) { return 2 * x; }), [2, 4, 6], st, 'doubler mapper');

			st.test('generators', { skip: generators.length === 0 }, function (s2t) {
				forEach(generators, function (gen) {
					s2t.doesNotThrow(
						function () { map(gen(), function () {}); },
						'generator function ' + debug(gen) + ' does not need to be from-wrapped first'
					);
				});

				s2t.end();
			});

			st.test('262: mapper-this', function (s2t) {
				var thisValue;
				var iter = map(
					[1][Symbol.iterator](),
					function () {
						thisValue = this;
						return 42;
					}
				);
				iter.next();
				s2t.equal(thisValue, undefined, 'mapper this is undefined in sloppy mode');

				s2t.end();
			});

			st.test('262: result is iterator', function (s2t) {
				var mappedIter = map([1, 2][Symbol.iterator](), function (x) { return x; });
				s2t.equal(typeof mappedIter.next, 'function', 'has next method');
				s2t.equal(typeof mappedIter[Symbol.iterator], 'function', 'has Symbol.iterator method');
				s2t.equal(mappedIter[Symbol.iterator](), mappedIter, 'Symbol.iterator returns itself');

				s2t.end();
			});

			st.test('262: get next method only once', { skip: !hasPropertyDescriptors }, function (s2t) {
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

				var iter = map(testIter, function (x) { return x; });
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
					function () { map(testIter, function () {}); },
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

				var iter = map(badIterator, function (x) { return x; });
				s2t['throws'](function () { iter.next(); }, TypeError, 'throws when next returns null');

				var badIterator2 = {
					next: function () {
						return 42;
					}
				};

				var iter2 = map(badIterator2, function (x) { return x; });
				s2t['throws'](function () { iter2.next(); }, TypeError, 'throws when next returns number');

				s2t.end();
			});

			st.test('262: next method returns throwing done', { skip: !hasPropertyDescriptors }, function (s2t) {
				var throwingIterator = {
					next: function () {
						var iterResult = { value: 1 };
						Object.defineProperty(iterResult, 'done', {
							get: function () {
								throw new EvalError('done getter threw');
							}
						});
						return iterResult;
					}
				};

				var iter = map(throwingIterator, function (x) { return x; });
				s2t['throws'](function () { iter.next(); }, EvalError, 'throws when done getter throws');

				s2t.end();
			});

			st.test('262: next method returns throwing value', { skip: !hasPropertyDescriptors }, function (s2t) {
				var throwingIterator = {
					next: function () {
						var iterResult = { done: false };
						Object.defineProperty(iterResult, 'value', {
							get: function () {
								throw new EvalError('value getter threw');
							}
						});
						return iterResult;
					}
				};

				var iter = map(throwingIterator, function (x) { return x; });
				s2t['throws'](function () { iter.next(); }, EvalError, 'throws when value getter throws');

				s2t.end();
			});

			st.test('262: next method throws', function (s2t) {
				var throwingIterator = {
					next: function () {
						throw new EvalError('next threw');
					}
				};

				var iter = map(throwingIterator, function (x) { return x; });
				s2t['throws'](function () { iter.next(); }, EvalError, 'throws error from next');

				s2t.end();
			});

			st.test('262: mapper throws', function (s2t) {
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

				var iter = map(testIter, function () {
					throw new SyntaxError('mapper threw');
				});

				s2t['throws'](function () { iter.next(); }, SyntaxError, 'throws error from mapper');
				s2t.equal(returnCalls, 1, 'return called on underlying iterator');

				s2t.end();
			});

			st.test('262: mapper throws then closing iterator also throws', function (s2t) {
				var testIter = {
					next: function () {
						return { done: false, value: 1 };
					},
					'return': function () {
						throw new EvalError('return threw');
					}
				};

				var iter = map(testIter, function () {
					throw new SyntaxError('mapper threw');
				});

				s2t['throws'](function () { iter.next(); }, SyntaxError, 'mapper exception wins over return exception');

				s2t.end();
			});

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

				var iter = map(badIterator, function (x) { return x; });
				iter.next();

				s2t['throws'](function () { iter['return'](); }, SyntaxError, 'throws error from return getter');

				s2t.end();
			});

			st.test('262: return is forwarded to underlying iterator', function (s2t) {
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

				var iter = map(testIter, function (x) { return x; });
				iter.next();
				s2t.equal(returnCalls, 0, 'return not called before calling return()');
				iter['return']();
				s2t.equal(returnCalls, 1, 'return called once');

				s2t.end();
			});

			st.test('262: return on suspended helper returns done and closes underlying', function (s2t) {
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

				var iter = map(testIter, function (x) { return x * 2; });
				var first = iter.next();
				s2t.deepEqual(first, { value: 2, done: false }, 'first next returns mapped value');
				s2t.equal(returnCalls, 0, 'return not yet called');

				var returnResult = iter['return']();
				s2t.deepEqual(returnResult, { value: undefined, done: true }, 'return() returns {value: undefined, done: true}');
				s2t.equal(returnCalls, 1, 'underlying return called exactly once');

				s2t.end();
			});

			st.test('262: return is not forwarded after exhaustion', function (s2t) {
				var returnCalls = 0;

				var testIter = {
					next: function () {
						return { done: true, value: undefined };
					},
					'return': function () {
						returnCalls += 1;
						throw new EvalError('return should not be called');
					}
				};

				var iter = map(testIter, function (x) { return x; });
				iter.next();
				iter['return']();
				s2t.equal(returnCalls, 0, 'return not called after exhaustion');

				s2t.end();
			});

			st.test('262: iterator already exhausted', function (s2t) {
				var testIter = {
					next: function () {
						return { done: true, value: undefined };
					}
				};

				var mapperCalls = 0;
				var iter = map(testIter, function (x) {
					mapperCalls += 1;
					return x;
				});

				var nextResult = iter.next();
				s2t.equal(nextResult.done, true, 'done is true');
				s2t.equal(nextResult.value, undefined, 'value is undefined');
				s2t.equal(mapperCalls, 0, 'mapper not called for exhausted iterator');

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

				reentrantIterator = map(testIter, function (x) { return x; });
				s2t['throws'](function () { reentrantIterator.next(); }, TypeError, 'throws on reentrant next()');

				s2t.end();
			});

			st.test('262: underlying iterator closed when mapper returns abrupt', function (s2t) {
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

				var iter = map(testIter, function () {
					throw new EvalError('mapper threw');
				});

				s2t['throws'](function () { iter.next(); }, EvalError);
				s2t.equal(returnCalls, 1, 'underlying iterator closed on mapper throw');

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
			t.equal(typeof Iterator.prototype[fnName], 'function', 'exists and is a function');

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
