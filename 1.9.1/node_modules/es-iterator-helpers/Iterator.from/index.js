'use strict';

var callBind = require('call-bind');

var getPolyfill = require('./polyfill');

var bound = callBind(getPolyfill(), null);

module.exports = bound;
