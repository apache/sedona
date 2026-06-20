'use strict';

var RequireObjectCoercible = require('es-object-atoms/RequireObjectCoercible');
var ToString = require('es-abstract/2025/ToString');
var callBound = require('call-bound');
var safeRegexTester = require('safe-regex-test');

var $replace = callBound('String.prototype.replace');
var $charAt = callBound('String.prototype.charAt');
var $slice = callBound('String.prototype.slice');

var mvsIsWS = (/^\s$/).test('\u180E');
/* eslint-disable no-control-regex */
var leftWhitespace = mvsIsWS
	? /^[\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u180E\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF]+/
	: /^[\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF]+/;
var isWhitespace = safeRegexTester(mvsIsWS
	? /[\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u180E\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF]$/
	: /[\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF]$/);
/* eslint-enable no-control-regex */

module.exports = function trim() {
	var S = $replace(ToString(RequireObjectCoercible(this)), leftWhitespace, '');
	// scan back for the trailing whitespace boundary; a `$`-anchored regexp is quadratic on large internal whitespace runs
	var end = S.length;
	while (end > 0 && isWhitespace($charAt(S, end - 1))) {
		end -= 1;
	}
	return $slice(S, 0, end);
};
