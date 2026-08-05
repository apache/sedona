var isCore = require('is-core-module');
var fs = require('fs');
var path = require('path');
var $Error = require('es-errors');
var $TypeError = require('es-errors/type');

var getHomedir = require('./homedir');
var caller = require('./caller');
var nodeModulesPaths = require('./node-modules-paths');
var normalizeOptions = require('./normalize-options');
var resolveExports = require('./exports-resolve');
var parsePackageSpecifier = require('./parse-package-specifier');
var getExportsCategory = require('./get-exports-category');
var getCategoryInfo = require('node-exports-info/getCategoryInfo');
var getCategoriesForRange = require('node-exports-info/getCategoriesForRange');
var selectMostRestrictive = require('./select-most-restrictive');

var realpathFS = process.platform !== 'win32' && fs.realpathSync && typeof fs.realpathSync.native === 'function' ? fs.realpathSync.native : fs.realpathSync;

var relativePathRegex = /^(?:\.\.?(?:\/|$)|\/|([A-Za-z]:)?[/\\])/;
var windowsDriveRegex = /^\w:[/\\]*$/;
var nodeModulesRegex = /[/\\]node_modules[/\\]*$/;

var homedir = getHomedir();
function defaultPaths() {
    if (!homedir) return [];
    return [
        path.join(homedir, '.node_modules'),
        path.join(homedir, '.node_libraries')
    ];
}

var defaultIsFile = function isFile(file) {
    try {
        var stat = fs.statSync(file, { throwIfNoEntry: false });
    } catch (e) {
        if (e && (e.code === 'ENOENT' || e.code === 'ENOTDIR')) return false;
        throw e;
    }
    return !!stat && (stat.isFile() || stat.isFIFO());
};

var defaultIsDir = function isDirectory(dir) {
    try {
        var stat = fs.statSync(dir, { throwIfNoEntry: false });
    } catch (e) {
        if (e && (e.code === 'ENOENT' || e.code === 'ENOTDIR')) return false;
        throw e;
    }
    return !!stat && stat.isDirectory();
};

var defaultRealpathSync = function realpathSync(x) {
    try {
        return realpathFS(x);
    } catch (realpathErr) {
        if (realpathErr.code !== 'ENOENT') {
            throw realpathErr;
        }
    }
    return x;
};

function maybeRealpathSync(realpathSync, x, opts) {
    if (!opts || !opts.preserveSymlinks) {
        return realpathSync(x);
    }
    return x;
}

function defaultReadPackageSync(readFileSync, pkgfile) {
    return JSON.parse(readFileSync(pkgfile));
}

function getPackageCandidates(x, start, opts) {
    var dirs = nodeModulesPaths(start, opts, x);
    for (var i = 0; i < dirs.length; i++) {
        dirs[i] = path.join(dirs[i], x);
    }
    return dirs;
}

function findConsumerPackageSync(startDir, isFile, readPackageSync, readFileSync) {
    var dir = path.resolve(startDir);
    while (true) {
        var pkgfile = path.join(dir, 'package.json');
        if (isFile(pkgfile)) {
            try {
                return readPackageSync(readFileSync, pkgfile);
            } catch (e) {
                if (!(e instanceof SyntaxError)) {
                    throw e;
                }
            }
        }
        var parentDir = path.dirname(dir);
        if (parentDir === dir) {
            return null;
        }
        dir = parentDir;
    }
}

function findPackageWithDirSync(startDir, isFile, readPackageSync, readFileSync) {
    var dir = path.resolve(startDir);
    while (true) {
        // Stop at node_modules boundaries - can't self-reference across node_modules
        if (nodeModulesRegex.test(dir)) {
            return null;
        }
        var pkgfile = path.join(dir, 'package.json');
        if (isFile(pkgfile)) {
            try {
                var pkg = readPackageSync(readFileSync, pkgfile);
                return {
                    __proto__: null, pkg: pkg, dir: dir
                };
            } catch (e) {
                if (!(e instanceof SyntaxError)) {
                    throw e;
                }
            }
        }
        var parentDir = path.dirname(dir);
        if (parentDir === dir) {
            return null;
        }
        dir = parentDir;
    }
}

module.exports = function resolveSync(x, options) {
    if (typeof x !== 'string') {
        throw new $TypeError('Path must be a string.');
    }
    var opts = normalizeOptions(x, options);

    var isFile = opts.isFile || defaultIsFile;
    var isDirectory = opts.isDirectory || defaultIsDir;
    var readFileSync = opts.readFileSync || fs.readFileSync;
    var realpathSync = opts.realpathSync || defaultRealpathSync;
    var readPackageSync = opts.readPackageSync || defaultReadPackageSync;
    if (opts.readFileSync && opts.readPackageSync) {
        throw new $TypeError('`readFileSync` and `readPackageSync` are mutually exclusive.');
    }
    var packageIterator = opts.packageIterator;

    if (typeof opts.moduleSystem !== 'undefined' && opts.moduleSystem !== 'require' && opts.moduleSystem !== 'import') {
        throw new $TypeError('`moduleSystem` must be `\'require\'` or `\'import\'`.');
    }

    var extensions = opts.extensions || ['.js'];
    var includeCoreModules = opts.includeCoreModules !== false;
    var basedir = opts.basedir || path.dirname(caller());
    var parent = opts.filename || basedir;

    opts.paths = opts.paths || defaultPaths();

    // Determine exports category
    var exportsCategory = getExportsCategory(opts);
    if (exportsCategory === 'engines') {
        // Read consumer's package.json to get engines.node
        var consumerPkg = findConsumerPackageSync(basedir, isFile, readPackageSync, readFileSync);
        if (consumerPkg && consumerPkg.engines && consumerPkg.engines.node) {
            var categories = getCategoriesForRange(consumerPkg.engines.node);
            exportsCategory = selectMostRestrictive(categories);
        } else {
            exportsCategory = null;
        }
    }

    var useExports = exportsCategory !== null && exportsCategory !== 'pre-exports';

    // ensure that `basedir` is an absolute path at this point, resolving against the process' current working directory
    var absoluteStart = maybeRealpathSync(realpathSync, path.resolve(basedir), opts);

    if (opts.basedir && !isDirectory(absoluteStart)) {
        var dirError = new $TypeError('Provided basedir "' + opts.basedir + '" is not a directory' + (opts.preserveSymlinks ? '' : ', or a symlink to a directory'));
        dirError.code = 'INVALID_BASEDIR';
        throw dirError;
    }

    if (relativePathRegex.test(x)) {
        var res = path.resolve(absoluteStart, x);
        if (x === '.' || x === '..' || x.slice(-1) === '/') res += '/';
        var m = loadAsFileSync(res) || loadAsDirectorySync(res);
        if (m) return maybeRealpathSync(realpathSync, m, opts);
    } else if (includeCoreModules && isCore(x)) {
        return x;
    } else if (useExports) {
        // Try self-reference resolution first
        var selfRef = resolveSelfReferenceSync(x, absoluteStart);
        if (selfRef) return maybeRealpathSync(realpathSync, selfRef, opts);
        var nE = loadNodeModulesWithExportsSync(x, absoluteStart);
        if (nE) return maybeRealpathSync(realpathSync, nE, opts);
    } else {
        var n = loadNodeModulesSync(x, absoluteStart);
        if (n) return maybeRealpathSync(realpathSync, n, opts);
    }

    var err = new $Error("Cannot find module '" + x + "' from '" + parent + "'");
    err.code = 'MODULE_NOT_FOUND';
    throw err;

    function resolveSelfReferenceSync(x, startDir) {
        var parsed = parsePackageSpecifier(x);
        var pkgInfo = findPackageWithDirSync(startDir, isFile, readPackageSync, readFileSync);

        if (!pkgInfo || !pkgInfo.pkg || pkgInfo.pkg.name !== parsed.name) {
            return null; // Not a self-reference
        }

        var pkg = pkgInfo.pkg;
        var pkgDir = pkgInfo.dir;

        if (opts.packageFilter) {
            pkg = opts.packageFilter(pkg, path.join(pkgDir, 'package.json'), pkgDir);
        }

        // If package has exports field, resolve via exports
        if (typeof pkg.exports !== 'undefined') {
            var categoryInfo = getCategoryInfo(exportsCategory, opts.moduleSystem || 'require');
            var conditions = opts.conditions || categoryInfo.conditions;
            var resolved = resolveExports(pkg.exports, parsed.subpath, conditions, categoryInfo.flags);
            if (resolved) {
                var resolvedPath = path.resolve(pkgDir, resolved);
                if (isFile(resolvedPath)) {
                    return resolvedPath;
                }
            }
            // exports field exists but didn't resolve - this is an error per Node semantics
            return undefined;
        }

        // No exports field - fall back to traditional resolution for self-reference
        // (Note: this matches Node's behavior where self-ref without exports uses main)
        if (parsed.subpath === '.') {
            return loadAsDirectorySync(pkgDir);
        }
        var subPath = path.join(pkgDir, parsed.subpath.slice(1));
        var sm = loadAsFileSync(subPath);
        if (sm) return sm;
        return loadAsDirectorySync(subPath);
    }

    function loadAsFileSync(x) {
        var pkg = loadpkg(path.dirname(x));

        if (pkg && pkg.dir && pkg.pkg && opts.pathFilter) {
            var rfile = path.relative(pkg.dir, x);
            var r = opts.pathFilter(pkg.pkg, x, rfile);
            if (r) {
                x = path.resolve(pkg.dir, r); // eslint-disable-line no-param-reassign
            }
        }

        if (isFile(x)) {
            return x;
        }

        for (var i = 0; i < extensions.length; i++) {
            var file = x + extensions[i];
            if (isFile(file)) {
                return file;
            }
        }
    }

    function loadpkg(dir) {
        if (dir === '' || dir === '/') return;
        if (process.platform === 'win32' && windowsDriveRegex.test(dir)) {
            return;
        }
        if (nodeModulesRegex.test(dir)) return;

        var pkgfile = path.join(isDirectory(dir) ? maybeRealpathSync(realpathSync, dir, opts) : dir, 'package.json');

        if (!isFile(pkgfile)) {
            return loadpkg(path.dirname(dir));
        }

        var pkg;
        try {
            pkg = readPackageSync(readFileSync, pkgfile);
        } catch (e) {
            if (!(e instanceof SyntaxError)) {
                throw e;
            }
        }

        if (pkg && opts.packageFilter) {
            pkg = opts.packageFilter(pkg, pkgfile, dir);
        }

        return { pkg: pkg, dir: dir };
    }

    function loadAsDirectorySync(x) {
        var pkgfile = path.join(isDirectory(x) ? maybeRealpathSync(realpathSync, x, opts) : x, '/package.json');
        if (isFile(pkgfile)) {
            try {
                var pkg = readPackageSync(readFileSync, pkgfile);
            } catch (e) {}

            if (pkg && opts.packageFilter) {
                pkg = opts.packageFilter(pkg, pkgfile, x);
            }

            if (pkg && pkg.main) {
                if (typeof pkg.main !== 'string') {
                    var mainError = new $TypeError('package “' + pkg.name + '” `main` must be a string');
                    mainError.code = 'INVALID_PACKAGE_MAIN';
                    throw mainError;
                }
                if (pkg.main === '.' || pkg.main === './') {
                    pkg.main = 'index';
                }
                try {
                    var mainPath = path.resolve(x, pkg.main);
                    var m = loadAsFileSync(mainPath);
                    if (m) return m;
                    var n = loadAsDirectorySync(mainPath);
                    if (n) return n;
                    var checkIndex = loadAsFileSync(path.resolve(x, 'index'));
                    if (checkIndex) return checkIndex;
                } catch (e) { }
                var incorrectMainError = new $Error("Cannot find module '" + path.resolve(x, pkg.main) + "'. Please verify that the package.json has a valid \"main\" entry");
                incorrectMainError.code = 'INCORRECT_PACKAGE_MAIN';
                throw incorrectMainError;
            }
        }

        return loadAsFileSync(path.join(x, '/index'));
    }

    function loadNodeModulesSync(x, start) {
        var thunk = function () { return getPackageCandidates(x, start, opts); };
        var dirs = packageIterator ? packageIterator(x, start, thunk, opts) : thunk();

        for (var i = 0; i < dirs.length; i++) {
            var dir = dirs[i];
            if (isDirectory(path.dirname(dir))) {
                var m = loadAsFileSync(dir);
                if (m) return m;
                var n = loadAsDirectorySync(dir);
                if (n) return n;
            }
        }
    }

    function loadNodeModulesWithExportsSync(x, start) {
        var parsed = parsePackageSpecifier(x);
        var categoryInfo = getCategoryInfo(exportsCategory, opts.moduleSystem || 'require');
        var conditions = opts.conditions || categoryInfo.conditions;

        // Get candidate directories for the package name
        var thunk = function () { return getPackageCandidates(parsed.name, start, opts); };
        var dirs = packageIterator ? packageIterator(parsed.name, start, thunk, opts) : thunk();

        for (var i = 0; i < dirs.length; i++) {
            var pkgDir = dirs[i];
            if (isDirectory(pkgDir)) {
                var pkgfile = path.join(pkgDir, 'package.json');
                if (isFile(pkgfile)) {
                    var pkg;
                    try {
                        pkg = readPackageSync(readFileSync, pkgfile);
                    } catch (e) {
                        if (!(e instanceof SyntaxError)) {
                            throw e;
                        }
                        pkg = null;
                    }

                    if (pkg) {
                        if (opts.packageFilter) {
                            pkg = opts.packageFilter(pkg, pkgfile, pkgDir);
                        }

                        // If package has exports field, use exports resolution
                        if (typeof pkg.exports !== 'undefined') {
                            var resolved = resolveExports(pkg.exports, parsed.subpath, conditions, categoryInfo.flags);
                            if (resolved) {
                                var resolvedPath = path.resolve(pkgDir, resolved);
                                if (isFile(resolvedPath)) {
                                    return resolvedPath;
                                }
                            }
                            // exports field exists but didn't resolve - this is an error per Node semantics
                            // (don't fall through to main/index)
                            return undefined;
                        }

                        // No exports field, fall back to traditional resolution
                        if (parsed.subpath === '.') {
                            var result = loadAsDirectorySync(pkgDir);
                            if (result) { return result; }
                        } else {
                            var subPath = path.join(pkgDir, parsed.subpath.slice(1));
                            var sm = loadAsFileSync(subPath);
                            if (sm) { return sm; }
                            var sn = loadAsDirectorySync(subPath);
                            if (sn) { return sn; }
                        }
                    }
                } else if (parsed.subpath === '.') {
                    // No package.json, fall back to file/directory resolution
                    var m = loadAsFileSync(pkgDir);
                    if (m) { return m; }
                    var n = loadAsDirectorySync(pkgDir);
                    if (n) { return n; }
                } else {
                    // No package.json, fall back to file/directory resolution for subpath
                    var fullPath = path.join(pkgDir, parsed.subpath.slice(1));
                    var m2 = loadAsFileSync(fullPath);
                    if (m2) { return m2; }
                    var n2 = loadAsDirectorySync(fullPath);
                    if (n2) { return n2; }
                }
            }
        }
    }
};
