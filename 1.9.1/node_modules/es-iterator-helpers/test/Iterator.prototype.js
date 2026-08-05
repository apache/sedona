'use strict';

var defineProperties = require('define-properties');
var test = require('tape');
var hasSymbols = require('has-symbols')();
var hasToStringTag = require('has-tostringtag')();
var functionsHaveNames = require('functions-have-names')();
var functionsHaveConfigurableNames = require('functions-have-names').functionsHaveConfigurableNames();

var index = require('../Iterator.prototype');
var impl = require('../Iterator.prototype/implementation');

var isEnumerable = Object.prototype.propertyIsEnumerable;

var $Iterator = require('../Iterator/implementation');

module.exports = {
	tests: function (proto, name, t) {
		t.notEqual(proto, null, 'is not null');
		t.equal(typeof proto, 'object', 'is an object');

		t.test('Symbol.iterator', { skip: !hasSymbols }, function (st) {
			st.equal(typeof proto[Symbol.iterator], 'function', 'has a `Symbol.iterator` method');
			st.equal(
				proto[Symbol.iterator].name,
				'[Symbol.iterator]',
				'has name "[Symbol.iterator]"',
				{ skip: functionsHaveNames && !functionsHaveConfigurableNames }
			);
			st.equal(proto[Symbol.iterator](), proto, 'function returns proto');
			st.equal(proto[Symbol.iterator].call($Iterator), $Iterator, 'function returns receiver');

			st.end();
		});

		t.test('Symbol.toStringTag', { skip: !hasToStringTag }, function (st) {
			st.equal(proto[Symbol.toStringTag], 'Iterator', 'has a `Symbol.toStringTag` property');

			st.end();
		});
	},
	index: function () {
		test('Iterator.prototype: index', function (t) {
			module.exports.tests(index, 'Iterator.prototype', t);

			t.end();
		});
	},
	implementation: function () {
		test('Iterator.prototype: implementation', function (t) {
			module.exports.tests(impl, 'Iterator.prototype', t);

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator.prototype: shimmed', function (t) {
			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(Iterator, 'prototype'), 'Iterator.prototype is not enumerable');
				et.end();
			});

			module.exports.tests(Iterator.prototype, 'Iterator.prototype', t);

			t.test('262: Symbol.toStringTag accessor', { skip: !hasToStringTag || !defineProperties.supportsDescriptors }, function (st) {
				var desc = Object.getOwnPropertyDescriptor(Iterator.prototype, Symbol.toStringTag);

				st.equal(typeof desc.get, 'function', 'get is a function');
				st.equal(typeof desc.set, 'function', 'set is a function');
				st.equal(desc.configurable, true, 'is configurable');
				st.equal(desc.enumerable, false, 'is not enumerable');
				st.equal(desc.value, undefined, 'has no value');
				st.equal(desc.writable, undefined, 'has no writable');

				st.equal(desc.get.call(), 'Iterator', 'get returns "Iterator"');

				st['throws'](
					function () { desc.set.call(undefined, ''); },
					TypeError,
					'set throws when `this` is not an Object'
				);
				st['throws'](
					function () { desc.set.call(Iterator.prototype, ''); },
					TypeError,
					'set throws when `this` is %Iterator.prototype%'
				);
				st.equal(Iterator.prototype[Symbol.toStringTag], 'Iterator', 'toStringTag is unchanged after failed sets');

				var o = {};
				o[Symbol.toStringTag] = 'original';
				desc.set.call(o, 'sentinel');
				st.equal(o[Symbol.toStringTag], 'sentinel', 'set overwrites an own data property');
				st.equal(Iterator.prototype[Symbol.toStringTag], 'Iterator', '%Iterator.prototype% toStringTag is still "Iterator"');

				st.end();
			});

			t.end();
		});
	}
};
