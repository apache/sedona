'use strict';

var callBound = require('call-bound');

var toStr = callBound('Object.prototype.toString');

var isIE68 = !(0 in [,]); // eslint-disable-line no-sparse-arrays, no-magic-numbers

var objectClass = '[object Object]';
var ddaClass = '[object HTMLAllCollection]';

/** @type {(() => false) | ((value: unknown) => value is HTMLAllCollection)} */
var isDDA = function isDocumentDotAll() {
	return /** @type {const} */ (false);
};
if (typeof document === 'object') {
	// Firefox 3 canonicalizes DDA to undefined when it's not accessed directly
	var all = document.all;
	if (toStr(all) === toStr(document.all)) {
		isDDA = /** @type {import('.')} */ (function isDocumentDotAll(value) {
			/* globals document: false */
			// in IE 6-8, typeof document.all is "object" and it's truthy
			if ((isIE68 || !value) && (typeof value === 'undefined' || typeof value === 'object')) {
				try {
					// @ts-expect-error nullish will throw
					var str = toStr(value);
					// IE 6-8 uses `objectClass`
					return (str === ddaClass || str === objectClass)
						&& /** @type {Function} */ (value)('') == null; // eslint-disable-line eqeqeq
				} catch (e) { /**/ }
			}
			return false;
		});
	}
}

module.exports = isDDA;
