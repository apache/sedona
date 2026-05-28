'use strict';

var test = require('tape');
var forEach = require('for-each');

var getCategoriesForRange = require('../getCategoriesForRange');
var getCategory = require('../getCategory');

var boundaryVersions = require('./versions');

test('getCategoriesForRange', function (t) {
	t['throws'](
		function () { getCategoriesForRange('not a version'); },
		TypeError,
		'invalid version range throws'
	);

	t.deepEqual(
		getCategoriesForRange(process.version),
		[getCategory(process.version)],
		'exactly 1 category for the current version'
	);

	forEach(boundaryVersions, function (version) {
		t.test('boundary version: ' + version, function (st) {
			var categories = getCategoriesForRange(version);

			st.deepEqual(
				categories,
				[getCategory(version)],
				'exactly 1 category for a boundary version'
			);

			st.end();
		});
	});

	t.end();
});
