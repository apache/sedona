'use strict';

/** @type {(x: string) => { __proto__: null, name: string, subpath: string }} */
module.exports = function parsePackageSpecifier(x) {
    if (x.charAt(0) === '@') {
        var slashIndex = x.indexOf('/');
        if (slashIndex === -1) {
            return {
                __proto__: null, name: x, subpath: '.'
            };
        }
        var secondSlash = x.indexOf('/', slashIndex + 1);
        if (secondSlash === -1) {
            return {
                __proto__: null, name: x, subpath: '.'
            };
        }
        return {
            __proto__: null, name: x.slice(0, secondSlash), subpath: '.' + x.slice(secondSlash)
        };
    }
    var firstSlash = x.indexOf('/');
    if (firstSlash === -1) {
        return {
            __proto__: null, name: x, subpath: '.'
        };
    }
    return {
        __proto__: null, name: x.slice(0, firstSlash), subpath: '.' + x.slice(firstSlash)
    };
};
