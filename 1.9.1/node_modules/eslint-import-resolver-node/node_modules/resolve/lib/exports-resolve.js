'use strict';

var objectKeys = require('object-keys');
var $Error = require('es-errors');

// Check if an exports map key looks like a subpath (starts with '.')
function isSubpathKey(key) {
    return key.length > 0 && key.charAt(0) === '.';
}

// Normalize the exports field into a map of subpath -> target
function normalizeExports(exportsField) {
    if (typeof exportsField === 'string') {
        return { __proto__: null, '.': exportsField };
    }
    if (Array.isArray(exportsField)) {
        return { __proto__: null, '.': exportsField };
    }
    if (typeof exportsField === 'object' && exportsField !== null) {
        var keys = objectKeys(exportsField);
        if (keys.length === 0) {
            return { __proto__: null };
        }
        // If any key starts with '.', it's a subpath map
        // If no key starts with '.', it's a conditions object for '.'
        var hasSubpath = false;
        for (var i = 0; !hasSubpath && i < keys.length; i++) {
            if (isSubpathKey(keys[i])) {
                hasSubpath = true;
            }
        }
        // Copy to new object with null prototype
        var result = { __proto__: null };
        for (var j = 0; j < keys.length; j++) {
            result[keys[j]] = exportsField[keys[j]];
        }
        if (hasSubpath) {
            return result;
        }
        return { __proto__: null, '.': result };
    }
    return null;
}

// Resolve a target value through conditions
// conditions: array of condition strings, or null (broken: string/array only)
function resolveTarget(target, conditions) {
    if (typeof target === 'string') {
        return target;
    }

    if (target === null) {
        return null;
    }

    if (Array.isArray(target)) {
        for (var i = 0; i < target.length; i++) {
            var resolved = resolveTarget(target[i], conditions);
            if (resolved !== null && typeof resolved !== 'undefined') {
                return resolved;
            }
        }
        return null;
    }

    if (typeof target === 'object') {
        // If no conditions supported (broken category), can't resolve objects
        if (conditions === null) {
            return null;
        }
        var keys = objectKeys(target);
        for (var j = 0; j < keys.length; j++) {
            var key = keys[j];
            for (var k = 0; k < conditions.length; k++) {
                if (key === conditions[k]) {
                    var result = resolveTarget(target[key], conditions);
                    if (result != null) {
                        return result;
                    }
                }
            }
        }
        return null;
    }

    return null;
}

// Validate a resolved path
function validateTarget(target) {
    if (typeof target !== 'string') {
        return false;
    }
    if (target.slice(0, 2) !== './') {
        return false;
    }
    if (target.indexOf('/node_modules/') !== -1) {
        return false;
    }
    // Check for '..' path traversal
    var parts = target.split('/');
    for (var i = 0; i < parts.length; i++) {
        if (parts[i] === '..') {
            return false;
        }
    }
    return true;
}

// Find the best pattern match for a subpath among keys with '*'
function findPatternMatch(subpath, exportsMap, allowPatternTrailers) {
    var keys = objectKeys(exportsMap);
    var bestKey = null;
    var bestPrefixLen = -1;
    var bestMatch = '';

    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var starIndex = key.indexOf('*');
        // Key must have exactly one '*'
        if (starIndex !== -1 && key.indexOf('*', starIndex + 1) === -1) {
            var prefix = key.slice(0, starIndex);
            var suffix = key.slice(starIndex + 1);

            // Pattern trailers: if suffix is non-empty after *, need allowPatternTrailers
            if (suffix.length === 0 || allowPatternTrailers) {
                if (
                    subpath.length >= prefix.length + suffix.length
                    && subpath.slice(0, prefix.length) === prefix
                    && (suffix.length === 0 || subpath.slice(subpath.length - suffix.length) === suffix)
                ) {
                    // Longest prefix wins
                    if (prefix.length > bestPrefixLen) {
                        bestPrefixLen = prefix.length;
                        bestKey = key;
                        bestMatch = subpath.slice(prefix.length, subpath.length - suffix.length);
                    }
                }
            }
        }
    }

    if (bestKey !== null) {
        return {
            __proto__: null, key: bestKey, match: bestMatch
        };
    }
    return null;
}

// Find directory slash match (for categories that support it)
function findDirSlashMatch(subpath, exportsMap) {
    var keys = objectKeys(exportsMap);
    var bestKey = null;
    var bestPrefixLen = -1;

    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        if (key.charAt(key.length - 1) === '/') {
            if (subpath.slice(0, key.length) === key && key.length > bestPrefixLen) {
                bestPrefixLen = key.length;
                bestKey = key;
            }
        }
    }

    if (bestKey !== null) {
        return {
            __proto__: null, key: bestKey, remainder: subpath.slice(bestKey.length)
        };
    }
    return null;
}

// Replace '*' in target string with match value
function substitutePattern(target, match) {
    if (typeof target === 'string') {
        return target.split('*').join(match);
    }
    if (Array.isArray(target)) {
        var result = [];
        for (var i = 0; i < target.length; i++) {
            result.push(substitutePattern(target[i], match));
        }
        return result;
    }
    if (typeof target === 'object' && target !== null) {
        var obj = { __proto__: null };
        var keys = objectKeys(target);
        for (var j = 0; j < keys.length; j++) {
            obj[keys[j]] = substitutePattern(target[keys[j]], match);
        }
        return obj;
    }
    return target;
}

// Main exports resolution function
// exportsField: the value of package.json "exports"
// subpath: the subpath to resolve (e.g., "." or "./foo/bar")
// conditions: array of condition strings, or null for broken category
// options: { patterns: boolean, patternTrailers: boolean, dirSlash: boolean }
// Returns: resolved relative path string, or null if no exports field
// Throws: when exports field exists but subpath is not exported
module.exports = function resolveExports(exportsField, subpath, conditions, options) {
    if (typeof exportsField === 'undefined') {
        return null;
    }

    var exportsMap = normalizeExports(exportsField);
    if (!exportsMap) {
        return null;
    }

    var allowPatterns = options && options.patterns;
    var allowPatternTrailers = options && options.patternTrailers;
    var allowDirSlash = options && options.dirSlash;

    // 1. Exact key match
    if (typeof exportsMap[subpath] !== 'undefined') {
        var resolved = resolveTarget(exportsMap[subpath], conditions);
        if (resolved !== null && typeof resolved !== 'undefined') {
            if (!validateTarget(resolved)) {
                var invalidError = new $Error('Invalid "exports" target "' + resolved + '" for subpath "' + subpath + '"');
                invalidError.code = 'ERR_INVALID_PACKAGE_CONFIG';
                throw invalidError;
            }
            return resolved;
        }
        // Target exists but resolved to null (explicitly not exported)
        var notExportedError = new $Error('Package subpath "' + subpath + '" is not defined by "exports"');
        notExportedError.code = 'ERR_PACKAGE_PATH_NOT_EXPORTED';
        throw notExportedError;
    }

    // 2. Pattern match (keys with '*')
    if (allowPatterns) {
        var patternResult = findPatternMatch(subpath, exportsMap, allowPatternTrailers);
        if (patternResult) {
            var substituted = substitutePattern(exportsMap[patternResult.key], patternResult.match);
            var patternResolved = resolveTarget(substituted, conditions);
            if (patternResolved !== null && typeof patternResolved !== 'undefined') {
                if (!validateTarget(patternResolved)) {
                    var patternInvalidError = new $Error('Invalid "exports" target "' + patternResolved + '" for subpath "' + subpath + '"');
                    patternInvalidError.code = 'ERR_INVALID_PACKAGE_CONFIG';
                    throw patternInvalidError;
                }
                return patternResolved;
            }
        }
    }

    // 3. Directory slash match (for older categories)
    if (allowDirSlash) {
        var dirResult = findDirSlashMatch(subpath, exportsMap);
        if (dirResult) {
            var dirTarget = resolveTarget(exportsMap[dirResult.key], conditions);
            if (dirTarget !== null && typeof dirTarget !== 'undefined' && typeof dirTarget === 'string') {
                var dirResolved = dirTarget + dirResult.remainder;
                if (!validateTarget(dirResolved)) {
                    var dirInvalidError = new $Error('Invalid "exports" target "' + dirResolved + '" for subpath "' + subpath + '"');
                    dirInvalidError.code = 'ERR_INVALID_PACKAGE_CONFIG';
                    throw dirInvalidError;
                }
                return dirResolved;
            }
        }
    }

    var err = new $Error('Package subpath "' + subpath + '" is not defined by "exports"');
    err.code = 'ERR_PACKAGE_PATH_NOT_EXPORTED';
    throw err;
};
