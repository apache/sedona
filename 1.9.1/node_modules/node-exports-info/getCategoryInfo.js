'use strict';

var getCategoryFlags = require('./getCategoryFlags');
var getConditionsForCategory = require('./getConditionsForCategory');

/** @type {import('./getCategoryInfo')} */
module.exports = function getCategoryInfo(category, moduleSystem) {
	var conditions = getConditionsForCategory(category, moduleSystem || 'require');
	var flags = getCategoryFlags(category);
	return { conditions: conditions, flags: flags };
};
