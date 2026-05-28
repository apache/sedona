'use strict';

var test = require('tape');
var forEach = require('for-each');
var semver = require('semver');

var getCategory = require('../getCategory');
var getRange = require('../getRange');

var boundaryVersions = require('./versions');

test('getCategory', function (t) {
	t['throws'](
		function () { getCategory('not a version'); },
		RangeError,
		'invalid version throws'
	);
	t['throws'](
		function () { getCategory('^1.2.3'); },
		RangeError,
		'semver range throws'
	);

	t.doesNotThrow(
		function () { getCategory(process.version); },
		'current node version has a category'
	);

	forEach(boundaryVersions, function (version) {
		t.test('boundary version: ' + version, function (st) {
			st.test('default version', function (s2t) {
				var origVersion = process.version;
				Object.defineProperty(process, 'version', { value: version });
				s2t.teardown(function () { Object.defineProperty(process, 'version', { value: origVersion }); });

				s2t.equal(
					getCategory(),
					getCategory(version),
					'category with an explicit version matches the defaulted process.version'
				);

				s2t.end();
			});

			var range = getRange(getCategory(version));
			st.ok(
				semver.satisfies(version, range),
				'version ' + version + ' satisfies range ' + range
			);

			st.end();
		});
	});

	t.end();
});
