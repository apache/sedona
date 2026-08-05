'use strict';

var test = require('tape');
var $defineProperty = require('es-define-property');
var mockProperty = require('mock-property');

var GetV = require('../GetV');

test('GetV', function (t) {
	t['throws'](
		function () { return GetV({ 7: 7 }, 7); },
		TypeError,
		'Throws a TypeError if `P` is not a property key'
	);

	var obj = { a: function () {} };
	t.equal(GetV(obj, 'a'), obj.a, 'returns property if it exists');
	t.equal(GetV(obj, 'b'), undefined, 'returns undefined if property does not exist');

	t.test(
		'getter observability of the receiver',
		{ skip: !$defineProperty || !Object.isExtensible(Number.prototype) },
		function (st) {
		/** @type {unknown[]} */
			var receivers = [];

			st.teardown(mockProperty(/** @type {Record<PropertyKey, unknown>} */ (/** @type {unknown} */ (Number.prototype)), 'foo', {
				get: /** @this {number | Number} */ function () {
					receivers.push(this);
				}
			}));

			GetV(42, 'foo');
			GetV(Object(42), 'foo');

			st.deepEqual(receivers, [42, Object(42)]);

			st.end();
		}
	);

	t.end();
});
