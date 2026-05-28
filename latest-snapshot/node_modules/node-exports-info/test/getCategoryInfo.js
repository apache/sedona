'use strict';

var test = require('tape');
var forEach = require('for-each');

var getCategoryInfo = require('../getCategoryInfo');
var getCategoryFlags = require('../getCategoryFlags');
var getConditionsForCategory = require('../getConditionsForCategory');
var getRangePairs = require('../getRangePairs');

test('getCategoryInfo', function (t) {
	t['throws'](
		// @ts-expect-error
		function () { getCategoryInfo('not a category'); },
		RangeError,
		'invalid category throws'
	);

	forEach(getRangePairs(), function (pair) {
		var category = pair[1];
		t.test('category: ' + category, function (st) {
			var info = getCategoryInfo(category);

			st.ok(
				info && typeof info === 'object',
				'returns an object'
			);
			st.ok(
				'conditions' in info,
				'has conditions property'
			);
			st.ok(
				'flags' in info && info.flags && typeof info.flags === 'object',
				'has flags object'
			);

			// Verify it matches individual function results
			st.deepEqual(
				info.conditions,
				getConditionsForCategory(category, 'require'),
				'conditions match getConditionsForCategory with require'
			);
			st.deepEqual(
				info.flags,
				getCategoryFlags(category),
				'flags match getCategoryFlags'
			);

			// Test with explicit moduleSystem
			var importInfo = getCategoryInfo(category, 'import');
			st.deepEqual(
				importInfo.conditions,
				getConditionsForCategory(category, 'import'),
				'import conditions match getConditionsForCategory with import'
			);

			var requireInfo = getCategoryInfo(category, 'require');
			st.deepEqual(
				requireInfo.conditions,
				getConditionsForCategory(category, 'require'),
				'require conditions match getConditionsForCategory with require'
			);

			st.end();
		});
	});

	t.end();
});
