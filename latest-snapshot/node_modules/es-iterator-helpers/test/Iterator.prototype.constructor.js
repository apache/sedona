'use strict';

var defineProperties = require('define-properties');
var test = require('tape');

var Index = require('../Iterator.prototype.constructor');
var Impl = require('../Iterator.prototype.constructor/implementation');

var $Iterator = require('../Iterator/polyfill')();

var isEnumerable = Object.prototype.propertyIsEnumerable;

module.exports = {
	tests: function (t, constructor, name) {
		t.equal(constructor, $Iterator, name + ' is Iterator');
	},
	index: function () {
		test('Iterator.prototype.constructor: index', function (t) {
			t.notEqual(Index, $Iterator, 'index is not Iterator itself');
			t.equal(typeof Index, 'function', 'index is a function');

			t['throws'](
				function () { Index(); }, // eslint-disable-line new-cap
				TypeError,
				'index throws when Call-ed'
			);

			t['throws'](
				function () { return new Index(); },
				TypeError,
				'index throws when Construct-ed'
			);

			t.end();
		});
	},
	implementation: function () {
		test('Iterator.prototype.constructor: implementation', function (t) {
			t.equal(Impl, $Iterator, 'implementation is Iterator itself');
			module.exports.tests(t, Impl, 'Iterator.prototype.constructor');

			t.end();
		});
	},
	shimmed: function () {
		test('Iterator.prototype.constructor: shimmed', function (t) {
			module.exports.tests(t, Iterator.prototype.constructor, 'Iterator.prototype.constructor');

			t.test('enumerability', { skip: !defineProperties.supportsDescriptors }, function (et) {
				et.equal(false, isEnumerable.call(Iterator.prototype, 'constructor'), 'Iterator#constructor is not enumerable');
				et.end();
			});

			t.test('262: accessor property', { skip: !defineProperties.supportsDescriptors }, function (at) {
				var desc = Object.getOwnPropertyDescriptor(Iterator.prototype, 'constructor');

				at.equal(typeof desc.get, 'function', 'get is a function');
				at.equal(typeof desc.set, 'function', 'set is a function');
				at.equal(desc.configurable, true, 'is configurable');
				at.equal(desc.enumerable, false, 'is not enumerable');
				at.equal(desc.value, undefined, 'has no value');
				at.equal(desc.writable, undefined, 'has no writable');

				at.equal(desc.get.call(), $Iterator, 'get returns %Iterator%');

				at['throws'](
					function () { desc.set.call(undefined, ''); },
					TypeError,
					'set throws when `this` is not an Object'
				);
				at['throws'](
					function () { desc.set.call(Iterator.prototype, ''); },
					TypeError,
					'set throws when `this` is %Iterator.prototype%'
				);
				at.equal(Iterator.prototype.constructor, $Iterator, 'constructor is unchanged after failed sets');

				var o = { constructor: 'original' };
				desc.set.call(o, 'sentinel');
				at.equal(o.constructor, 'sentinel', 'set overwrites an own data property');
				at.equal(Iterator.prototype.constructor, $Iterator, '%Iterator.prototype% constructor is still %Iterator%');

				at.end();
			});

			t.end();
		});
	}
};
