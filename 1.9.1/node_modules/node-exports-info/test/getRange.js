'use strict';

var test = require('tape');
var forEach = require('for-each');

var getRange = require('../getRange');
var getRangePairs = require('../getRangePairs');

test('getRange', function (t) {
	t['throws'](
		// @ts-expect-error
		function () { getRange('not a category'); },
		RangeError,
		'invalid category throws'
	);

	forEach(getRangePairs(), function (entry) {
		var range = entry[0];
		var category = entry[1];

		var actualRange = getRange(category);
		t.equal(actualRange, range, 'yielded range for ' + category + ' is as expected');
	});

	t.end();
});
