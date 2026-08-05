'use strict';

var $RangeError = require('es-errors/range');
var $TypeError = require('es-errors/type');

var isCategory = require('./isCategory');

// pre-computed condition sets
/** @type {['import', 'node', 'require', 'default']} */
var base = [
	'import',
	'node',
	'require',
	'default'
];
/** @type {['import', 'node', 'default']} */
var baseImport = [
	'import',
	'node',
	'default'
];
/** @type {['node', 'require', 'default']} */
var baseRequire = [
	'node',
	'require',
	'default'
];
/** @type {['import', 'node-addons', 'node', 'require', 'default']} */
var withAddons = [
	'import',
	'node-addons',
	'node',
	'require',
	'default'
];
/** @type {['import', 'node-addons', 'node', 'default']} */
var withAddonsImport = [
	'import',
	'node-addons',
	'node',
	'default'
];
/** @type {['node-addons', 'node', 'require', 'default']} */
var withAddonsRequire = [
	'node-addons',
	'node',
	'require',
	'default'
];
/** @type {['import', 'node-addons', 'node', 'require', 'module-sync', 'default']} */
var withAddonsModuleSync = [
	'import',
	'node-addons',
	'node',
	'require',
	'module-sync',
	'default'
];
/** @type {['import', 'node-addons', 'node', 'module-sync', 'default']} */
var withAddonsModuleSyncImport = [
	'import',
	'node-addons',
	'node',
	'module-sync',
	'default'
];
/** @type {['node-addons', 'node', 'require', 'module-sync', 'default']} */
var withAddonsModuleSyncRequire = [
	'node-addons',
	'node',
	'require',
	'module-sync',
	'default'
];

// categories that support node-addons condition (added in v14.19/v16.10)
/** @type {{ [k: string]: boolean | null | undefined }} */
var nodeAddonsCategories = {
	__proto__: null,
	'pattern-trailers': true,
	'pattern-trailers+json-imports': true,
	'pattern-trailers-no-dir-slash': true,
	'pattern-trailers-no-dir-slash+json-imports': true,
	'require-esm': true,
	'strips-types': true,
	'subpath-imports-slash': true
};

// categories that support module-sync condition (added in v22.12)
/** @type {{ [k: string]: boolean | null | undefined }} */
var moduleSyncCategories = {
	__proto__: null,
	'require-esm': true,
	'strips-types': true,
	'subpath-imports-slash': true
};

/** @type {import('./getConditionsForCategory')} */
module.exports = function getConditionsForCategory(category) {
	if (!isCategory(category)) {
		throw new $RangeError('invalid category ' + category);
	}

	var moduleSystem = arguments.length > 1 ? arguments[1] : null;
	if (arguments.length > 1 && moduleSystem !== 'import' && moduleSystem !== 'require') {
		throw new $TypeError('invalid moduleSystem: must be `\'require\'` or `\'import\'` if provided, got' + moduleSystem);
	}

	if (category === 'experimental') {
		return ['default'];
	}
	if (category === 'broken' || category === 'pre-exports') {
		return null;
	}

	var hasAddons = !!nodeAddonsCategories[category];
	var hasModuleSync = !!moduleSyncCategories[category];

	if (hasAddons && hasModuleSync) {
		return moduleSystem === 'import' ? withAddonsModuleSyncImport : moduleSystem === 'require' ? withAddonsModuleSyncRequire : withAddonsModuleSync;
	}
	if (hasAddons) {
		return moduleSystem === 'import' ? withAddonsImport : moduleSystem === 'require' ? withAddonsRequire : withAddons;
	}
	return moduleSystem === 'import' ? baseImport : moduleSystem === 'require' ? baseRequire : base;
};
