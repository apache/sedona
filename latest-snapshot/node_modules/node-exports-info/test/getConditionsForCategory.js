'use strict';

var test = require('tape');
var forEach = require('for-each');

var getConditionsForCategory = require('../getConditionsForCategory');
var getRangePairs = require('../getRangePairs');

test('getConditionsForCategory', function (t) {
	t['throws'](
		// @ts-expect-error
		function () { getConditionsForCategory('not a category'); },
		RangeError,
		'invalid category throws'
	);

	forEach(getRangePairs(), function (pair) {
		var category = pair[1];
		t.test('category: ' + category, function (st) {
			if (category === 'broken' || category === 'pre-exports') {
				st.equal(
					getConditionsForCategory(category),
					null,
					'category that does not support conditions, yields null'
				);
			} else {
				var conditions = getConditionsForCategory(category);
				st.ok(Array.isArray(conditions), 'moduleSystem none: returns an array');

				var requireConditions = getConditionsForCategory(category, 'require');
				st.ok(Array.isArray(requireConditions), 'moduleSystem require: returns an array');

				var importConditions = getConditionsForCategory(category, 'import');
				st.ok(Array.isArray(importConditions), 'moduleSystem import: returns an array');

				st['throws'](
					// @ts-expect-error
					function () { getConditionsForCategory(category, 'not a thing'); },
					TypeError,
					'invalid moduleSystem throws'
				);
			}

			st.end();
		});
	});

	t.end();
});
