'use strict';

var implementation = require('./implementation');

module.exports = function getPolyfill() {
	if (typeof Iterator === 'function' && typeof Iterator.prototype.filter === 'function') {
		try {
			// https://issues.chromium.org/issues/336839115
			Iterator.prototype.filter.call({ next: null }, function () {}).next();
		} catch (e) {
			var earlyCloseCount = 0;
			try {
				Iterator.prototype.filter.call(
					{
						next: function () {},
						'return': function () {
							earlyCloseCount += 1;
							return {};
						}
					},
					null
				);
			} catch (e2) { /**/ }
			if (earlyCloseCount > 0) {
				return Iterator.prototype.filter;
			}
		}
	}
	return implementation;
};
