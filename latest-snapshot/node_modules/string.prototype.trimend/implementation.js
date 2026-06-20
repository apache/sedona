'use strict';

var callBound = require('call-bound');
var $slice = callBound('String.prototype.slice');
var $charAt = callBound('String.prototype.charAt');
var $test = callBound('RegExp.prototype.test');

var mvsIsWS = (/^\s$/).test('\u180E');
/* eslint-disable no-control-regex */
var isWhitespace = mvsIsWS
	? /* istanbul ignore next */ /[\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u180E\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF]$/
	: /[\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF]$/;
/* eslint-enable no-control-regex */

module.exports = function trimEnd() {
	var S = $slice(this, 0);
	var end = S.length;
	while (end > 0 && $test(isWhitespace, $charAt(S, end - 1))) {
		end -= 1;
	}
	return $slice(S, 0, end);
};
