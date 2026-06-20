'use strict';

var callBind = require('call-bind');

var implementation = require('./implementation');
var getPolyfill = require('./polyfill');
var shim = require('./shim');

var bound = callBind(implementation);

/** @type {import('.')} */
module.exports = bound;

module.exports.getPolyfill = getPolyfill;
module.exports.implementation = implementation;
module.exports.shim = shim;
