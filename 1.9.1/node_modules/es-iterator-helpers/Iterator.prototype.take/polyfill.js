'use strict';

var implementation = require('./implementation');

module.exports = function getPolyfill() {
	if (typeof Iterator === 'function' && typeof Iterator.prototype.take === 'function') {
		var earlyCloseCount = 0;
		try {
			Iterator.prototype.take.call(
				{
					next: function () {},
					'return': function () {
						earlyCloseCount += 1;
						return {};
					}
				},
				NaN
			);
		} catch (e) { /**/ }
		if (earlyCloseCount > 0) {
			return Iterator.prototype.take;
		}
	}
	return implementation;
};
