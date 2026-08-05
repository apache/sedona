'use strict';

var test = require('tape');
var forEach = require('for-each');
var v = require('es-value-fixtures');
var inspect = require('object-inspect');

var getRange = require('../getRange');
var getRangePairs = require('../getRangePairs');
var isCategory = require('../isCategory');

test('isCategory', function (t) {
	forEach(v.nonStrings, function (nonString) {
		t.equal(isCategory(nonString), false, inspect(nonString) + ' is not a category');
	});

	forEach(getRangePairs(), function (pair) {
		var category = pair[1];

		t.equal(isCategory(category), true, inspect(category) + ' is a category');
		t.doesNotThrow(function () { getRange(category); }, 'getRange does not throw for ' + inspect(category));
	});

	t.end();
});
