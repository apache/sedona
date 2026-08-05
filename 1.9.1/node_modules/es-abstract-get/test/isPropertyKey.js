'use strict';

var test = require('tape');
var forEach = require('for-each');
var v = require('es-value-fixtures');
var debug = require('object-inspect');

var isPropertyKey = require('../isPropertyKey');

/** @import { PropertyKey } from '../isPropertyKey' */

test('isPropertyKey', function (t) {
	forEach(v.nonPropertyKeys, function (nonPropertyKey) {
		t.equal(isPropertyKey(nonPropertyKey), false, debug(nonPropertyKey) + ' is not a Property Key');
	});

	var propertyKeys = /** @type {PropertyKey[]} */ ([]).concat(
		v.strings,
		v.hasSymbols ? v.symbols : []
	);
	forEach(propertyKeys, function (propertyKey) {
		t.equal(isPropertyKey(propertyKey), true, debug(propertyKey) + ' is a Property Key');
	});

	t.end();
});
