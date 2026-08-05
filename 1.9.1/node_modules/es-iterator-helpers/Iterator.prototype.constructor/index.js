'use strict';

var bind = require('function-bind');

var getPolyfill = require('./polyfill');

var polyfill = bind.call(getPolyfill());

module.exports = polyfill;
