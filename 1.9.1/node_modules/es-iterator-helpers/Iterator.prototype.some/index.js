'use strict';

var callBind = require('call-bind');

var getPolyfill = require('./polyfill');

var polyfill = callBind(getPolyfill());

module.exports = polyfill;
