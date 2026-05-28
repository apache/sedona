'use strict';

// Category ranking from most restrictive (lowest rank) to least restrictive (highest rank)
// Lower rank = more restrictive = fewer features supported
var categoryRank = /** @type {const} */ {
    __proto__: null,
    'pre-exports': /** @type {const} */ (0),
    broken: /** @type {const} */ (1),
    experimental: /** @type {const} */ (2),
    conditions: /** @type {const} */ (3),
    'broken-dir-slash-conditions': /** @type {const} */ (4),
    patterns: /** @type {const} */ (5),
    'pattern-trailers': /** @type {const} */ (6),
    'pattern-trailers+json-imports': /** @type {const} */ (7),
    'pattern-trailers-no-dir-slash': /** @type {const} */ (8),
    'pattern-trailers-no-dir-slash+json-imports': /** @type {const} */ (9),
    'require-esm': /** @type {const} */ (10),
    'strips-types': /** @type {const} */ (11),
    'subpath-imports-slash': /** @type {const} */ (12)
};

// Select the most restrictive category from an array of categories
/** @type {(categories?: ReturnType<import('node-exports-info/getCategory')>[]) => import('node-exports-info/getCategory').Category | null} */
module.exports = function selectMostRestrictive(categories) {
    if (!categories || categories.length === 0) {
        return null;
    }

    var mostRestrictive = null;
    var lowestRank = Infinity;

    for (var i = 0; i < categories.length; i++) {
        var cat = categories[i];
        var rank = categoryRank[cat];
        if (typeof rank === 'number' && rank < lowestRank) {
            lowestRank = rank;
            mostRestrictive = cat;
        }
    }

    return mostRestrictive;
};
