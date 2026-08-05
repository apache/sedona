import implementation = require('./implementation');

/**
 * Gets the most compliant `Function.prototype.name` getter implementation to use.
 */
declare function getPolyfill(): typeof implementation;

export = getPolyfill;
