'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var functionsHaveNames = require('functions-have-names')();
var mockProperty = require('mock-property');
var hasProto = require('has-proto')();
var requireStash = require('require-stash');

var index = require('../Iterator');
var impl = require('../Iterator/implementation');

var isEnumerable = Object.prototype.propertyIsEnumerable;

module.exports = {
	tests: function (Iter, name, t) {
		t.equal(typeof Iter, 'function', name + ' is a function');

		t['throws'](
			function () { Iter(); }, // eslint-disable-line new-cap
			TypeError,
			name + ' throws when Call-ed'
		);

		t['throws'](
			function () { return new Iter(); },
			TypeError,
			name + ' throws when Construct-ed'
		);

		var SubIter;
		var SubSubIter;
		try {
			/* eslint no-new-func: 0 */
			SubIter = Function('Iter', 'return class SubIter extends Iter {};')(Iter);
			SubSubIter = Function('SubIter', 'return class SubSubIter extends SubIter {};')(SubIter);
		} catch (e) { /**/ }

		t.test('class inheritance', { skip: !SubIter }, function (st) {
			st.doesNotThrow(
				function () { return new SubIter(); },
				'Extending ' + name + ' does not throw when Construct-ed'
			);
			st.doesNotThrow(
				function () { return new SubSubIter(); },
				'Extending ' + name + ' twice does not throw when Construct-ed'
			);

			st.end();
		});
	},
	index: function () {
		test('Iterator: index', function (t) {
			module.exports.tests(index, 'Iterator', t);

			t.end();
		});
	},
	implementation: function () {
		test('Iterator: implementation', function (t) {
			module.exports.tests(impl, 'Iterator', t);

			t.end();
		});

		test('Iterator: implementation polyfill path', { skip: typeof Iterator !== 'function' }, function (t) {
			var restore = mockProperty(global, 'Iterator', { 'delete': true });

			// evict the module cache so implementation re-evaluates without native Iterator
			var restoreImpl = requireStash(__dirname, '../Iterator/implementation');

			var polyfillImpl = require('../Iterator/implementation'); // eslint-disable-line global-require

			t.equal(typeof polyfillImpl, 'function', 'polyfill is a function');

			t['throws'](
				function () { polyfillImpl(); },
				TypeError,
				'polyfill throws when Call-ed (not instanceof)'
			);

			t['throws'](
				function () { return new polyfillImpl(); }, // eslint-disable-line new-cap
				TypeError,
				'polyfill throws when Construct-ed directly'
			);

			t.test('polyfill prototype chain check', { skip: !hasProto }, function (st) {
				function BadSub() {}
				BadSub.prototype = Object.create(polyfillImpl.prototype);
				Object.defineProperty(BadSub.prototype, 'constructor', { value: BadSub, configurable: true, writable: true });
				// do NOT set Object.setPrototypeOf(BadSub, polyfillImpl)
				// so polyfillImpl is NOT in BadSub's [[Prototype]] chain

				var instance = new BadSub();

				st['throws'](
					function () { polyfillImpl.call(instance); },
					TypeError,
					'polyfill throws when constructor lacks Iterator in its prototype chain'
				);

				st.end();
			});

			t.test('polyfill subclass succeeds', { skip: !hasProto }, function (st) {
				function GoodSub() { return polyfillImpl.call(this); }
				GoodSub.prototype = Object.create(polyfillImpl.prototype);
				Object.defineProperty(GoodSub.prototype, 'constructor', { value: GoodSub, configurable: true, writable: true });
				Object.setPrototypeOf(GoodSub, polyfillImpl);

				st.doesNotThrow(
					function () { return new GoodSub(); },
					'polyfill does not throw for a proper subclass'
				);

				st.end();
			});

			// restore the module cache and native Iterator
			restoreImpl();
			restore();

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator: shimmed', function (t) {
			t.test('Function name', { skip: !functionsHaveNames }, function (st) {
				st.equal(Iterator.name, 'Iterator', 'Iterator has name "Iterator"');
				st.end();
			});

			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(global, Iterator), 'Iterator is not enumerable');
				et.end();
			});

			t.test('prototype descriptor', { skip: !defineProperties.supportsDescriptors }, function (pt) {
				var desc = Object.getOwnPropertyDescriptor(Iterator, 'prototype');
				pt.deepEqual(
					desc,
					{
						configurable: false,
						enumerable: false,
						value: Iterator.prototype,
						writable: false
					}
				);

				pt.end();
			});

			module.exports.tests(Iterator, 'Iterator', t);

			t.end();
		});
	}
};
