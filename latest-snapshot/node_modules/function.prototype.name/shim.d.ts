import implementation = require('./implementation');

/**
 * Installs a `Function.prototype.name` getter in engines where functions lack names, and returns the polyfill implementation.
 */
declare function shimFunctionPrototypeName(): typeof implementation;

export = shimFunctionPrototypeName;
