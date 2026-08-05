'use strict';

var test = require('tape');
var inspect = require('object-inspect');
var forEach = require('for-each');
var v = require('es-value-fixtures');

var isDocumentAll = require('../');

test('non-DDA values', function (t) {
	/** @type {unknown[]} */
	var nonDDAs = [].concat(
		// @ts-expect-error TS sucks with concat
		v.primitives,
		v.objects,
		[
			[],
			/a/g,
			new Date(),
			function () {}
		]
	);
	forEach(nonDDAs, function (nonDDA) {
		t.equal(isDocumentAll(nonDDA), false, inspect(nonDDA) + ' is not document.all');
	});

	t.end();
});

test('document.all', { skip: typeof document !== 'object' }, function (t) {
	t.plan(4);

	/* globals document: false */
	t.equal(isDocumentAll(document.all), true, 'document.all is document.all');
	t.equal(isDocumentAll(document), false, 'document is not document.all');

	var iframe = document.createElement('iframe');
	iframe.style.display = 'none'; // Optional: keep it hidden
	document.body.appendChild(iframe);
	t.teardown(function () { document.body.removeChild(iframe); });

	iframe.onload = function () {
		var doc = iframe.contentWindow && iframe.contentWindow.document;
		t.equal(
			isDocumentAll(doc && doc.all),
			true,
			'another realm’s document.all is document.all'
		);
		t.equal(isDocumentAll(doc), false, 'another realm’s document is not document.all');
	};
	iframe.src = 'about:blank'; // or point to a URL
});
