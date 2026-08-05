'use strict';

var test = require('tape');
var forEach = require('for-each');

var shims = require('..');

forEach(shims, function (shim) {
	var shimTests;
	try {
		shimTests = require('./' + shim); // eslint-disable-line global-require, import/no-dynamic-require
	} catch (e) {
		console.error(e);
		test(shim + ': index', { todo: true });
	}
	if (shimTests) {
		shimTests.index();
	}
});

