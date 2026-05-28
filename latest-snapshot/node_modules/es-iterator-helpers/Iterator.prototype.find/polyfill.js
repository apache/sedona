'use strict';

var implementation = require('./implementation');

module.exports = function getPolyfill() {
	if (typeof Iterator === 'function' && typeof Iterator.prototype.find === 'function') {
		var earlyCloseCount = 0;
		try {
			Iterator.prototype.find.call(
				{
					next: function () {},
					'return': function () {
						earlyCloseCount += 1;
						return {};
					}
				},
				null
			);
		} catch (e) { /**/ }
		if (earlyCloseCount > 0) {
			return Iterator.prototype.find;
		}
	}
	return implementation;
};
