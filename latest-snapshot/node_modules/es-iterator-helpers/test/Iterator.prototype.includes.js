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

var index = require('../Iterator.prototype.includes');
var impl = require('../Iterator.prototype.includes/implementation');

var fnName = 'includes';

var isEnumerable = Object.prototype.propertyIsEnumerable;

var testIterator = require('./helpers/testIterator');

module.exports = {
	tests: function (includes, name, t) {
		t['throws'](
			function () { return new includes(); }, // eslint-disable-line new-cap
			TypeError,
			'`' + name + '` is not a constructor'
		);

		forEach(v.primitives.concat(v.objects), function (nonIterator) {
			t['throws'](
				function () { iterate(includes(nonIterator)); },
				TypeError,
				debug(nonIterator) + ' is not an Object with a callable `next` method'
			);

			var badNext = { next: nonIterator };
			t['throws'](
				function () { iterate(includes(badNext)); },
				TypeError,
				debug(badNext) + ' is not an Object with a callable `next` method'
			);
		});

		forEach(v.nonIntegerNumbers, function (nonInteger) {
			t['throws'](
				function () { includes({ next: function () {} }, undefined, nonInteger); },
				TypeError,
				debug(nonInteger) + ' is not an integer number'
			);
		});

		forEach([-1, -2, -Infinity], function (negativeInteger) {
			t['throws'](
				function () { includes({ next: function () {} }, undefined, negativeInteger); },
				RangeError,
				debug(negativeInteger) + ' is not a positive integer number'
			);
		});

		t.test('Infinity skippedElements', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st.equal(includes(iterator(), 1, Infinity), false, 'Infinity skips everything, returns false');
			st.equal(includes(iterator(), 1, 0), true, '0 skips nothing, finds 1');

			st.end();
		});

		t.test('actual iteration', { skip: !hasSymbols }, function (st) {
			var arr = [1, 2, 3];
			var iterator = callBind(arr[Symbol.iterator], arr);

			st['throws'](
				function () { return new includes(iterator()); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);
			st['throws'](
				function () { return new includes(iterator(), 2); }, // eslint-disable-line new-cap
				TypeError,
				'`' + name + '` iterator is not a constructor'
			);

			testIterator(iterator(), [1, 2, 3], st, 'original');
			st.equal(includes(iterator(), 4), false, 'includes for always-false');
			st.equal(includes(iterator(), 1), true, 'includes for 1');
			st.equal(includes(iterator(), 1, 1), false, 'includes for 1, 1');
			st.equal(includes(iterator(), 2), true, 'includes for 2');
			st.equal(includes(iterator(), 2, 1), true, 'includes for 2, 1');
			st.equal(includes(iterator(), 2, 2), false, 'includes for 2, 2');
			st.equal(includes(iterator(), 3), true, 'includes for 3');
			st.equal(includes(iterator(), 3, 1), true, 'includes for 3, 1');
			st.equal(includes(iterator(), 3, 2), true, 'includes for 3, 2');
			st.equal(includes(iterator(), 3, 3), false, 'includes for 3, 3');

			st.test('test262: test/built-ins/Iterator/prototype/includes/iterator-already-exhausted', function (s2t) {
				var iter = [][Symbol.iterator]();
				var result = includes(iter);
				s2t.equal(result, false, 'includes returns false for empty iterator');

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

			t['throws'](
				function () { return new impl(); }, // eslint-disable-line new-cap
				TypeError,
				'`' + fnName + '` is not a constructor'
			);

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

			t['throws'](
				function () { return new Iterator.prototype[fnName](); },
				TypeError,
				'`' + fnName + '` is not a constructor'
			);

			module.exports.tests(callBind(Iterator.prototype[fnName]), 'Iterator.prototype.' + fnName, t);

			t.end();
		});
	}
};
