'use strict';

var test = require('tape');
var v = require('es-value-fixtures');

var Get = require('../Get');

test('Get', function (t) {
	t['throws'](
		// @ts-expect-error
		function () { return Get('a', 'a'); },
		TypeError,
		'Throws a TypeError if `O` is not an Object'
	);
	t['throws'](
		// @ts-expect-error
		function () { return Get({ 7: 7 }, 7); },
		TypeError,
		'Throws a TypeError if `P` is not a property key'
	);

	var sentinel = {};

	t.test('Symbols', { skip: !v.hasSymbols }, function (st) {
		var sym = Symbol('sym');
		var obj = /** @type {Record<symbol, unknown>} */ ({});
		obj[sym] = sentinel;

		st.equal(Get(obj, sym), sentinel, 'returns property `P` if it exists on object `O`');

		st.end();
	});

	t.equal(Get({ a: sentinel }, 'a'), sentinel, 'returns property `P` if it exists on object `O`');
	t.equal(
		// @ts-expect-error
		Get({}, 'a'),
		undefined,
		'returns undefined if property `P` does not exist on object `O`'
	);

	t.end();
});
