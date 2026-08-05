import implementation = require('./implementation');
import getPolyfill = require('./polyfill');
import shim = require('./shim');

/**
 * Returns the name of the given function.
 *
 * @param fn - The function whose name to retrieve.
 * @returns The name of the function.
 */
declare function getName(fn: Function): string;

declare namespace getName {
	export {
		getPolyfill,
		implementation,
		shim,
	};
}

export = getName;
