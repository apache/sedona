'use strict';

var test = require('tape');
var forEach = require('for-each');
var v = require('es-value-fixtures');
var debug = require('object-inspect');

var GetMethod = require('../GetMethod');

test('GetMethod', function (t) {
	forEach(v.nonPropertyKeys, function (nonPropertyKey) {
		t['throws'](
			// @ts-expect-error
			function () { return GetMethod({}, nonPropertyKey); },
			TypeError,
			debug(nonPropertyKey) + ' is not a Property Key'
		);
	});

	t['throws'](
		// @ts-expect-error
		function () { return GetMethod({ 7: 7 }, 7); },
		TypeError,
		'Throws a TypeError if `P` is not a property key'
	);

	t.equal(GetMethod({}, 'a'), undefined, 'returns undefined if property is undefined');
	t.equal(GetMethod({ a: null }, 'a'), undefined, 'returns undefined if property is null');
	t.equal(GetMethod({ a: undefined }, 'a'), undefined, 'returns undefined if property is undefined');

	var obj = { a: function () {} };
	t['throws'](
		// @ts-expect-error
		function () { GetMethod({ a: 'b' }, 'a'); },
		TypeError,
		'throws a TypeError if property exists and is not callable'
	);

	t.equal(GetMethod(obj, 'a'), obj.a, 'returns property if it is callable');

	t.end();
});
