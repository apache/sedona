'use strict';

/** @import { primitive } from '../' */
/** @import { primitiveES5 } from '../es5' */

/** @type {<T extends primitive | primitiveES5>(value: unknown) => value is T} */
module.exports = function isPrimitive(value) {
	return value === null || (typeof value !== 'function' && typeof value !== 'object');
};
