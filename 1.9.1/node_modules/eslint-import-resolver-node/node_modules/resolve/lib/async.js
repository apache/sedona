var fs = require('fs');
var getHomedir = require('./homedir');
var path = require('path');
var caller = require('./caller');
var nodeModulesPaths = require('./node-modules-paths');
var normalizeOptions = require('./normalize-options');
var isCore = require('is-core-module');
var $Error = require('es-errors');
var $TypeError = require('es-errors/type');
var getCategoryInfo = require('node-exports-info/getCategoryInfo');
var getCategoriesForRange = require('node-exports-info/getCategoriesForRange');

var resolveExports = require('./exports-resolve');
var parsePackageSpecifier = require('./parse-package-specifier');
var getExportsCategory = require('./get-exports-category');
var selectMostRestrictive = require('./select-most-restrictive');

var realpathFS = process.platform !== 'win32' && fs.realpath && typeof fs.realpath.native === 'function' ? fs.realpath.native : fs.realpath;

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

var defaultIsFile = function isFile(file, cb) {
    fs.stat(file, function (err, stat) {
        if (!err) {
            return cb(null, stat.isFile() || stat.isFIFO());
        }
        if (err.code === 'ENOENT' || err.code === 'ENOTDIR') return cb(null, false);
        return cb(err);
    });
};

var defaultIsDir = function isDirectory(dir, cb) {
    fs.stat(dir, function (err, stat) {
        if (!err) {
            return cb(null, stat.isDirectory());
        }
        if (err.code === 'ENOENT' || err.code === 'ENOTDIR') return cb(null, false);
        return cb(err);
    });
};

var defaultRealpath = function realpath(x, cb) {
    realpathFS(x, function (realpathErr, realPath) {
        if (realpathErr && realpathErr.code !== 'ENOENT') cb(realpathErr);
        else cb(null, realpathErr ? x : realPath);
    });
};

function maybeRealpath(realpath, x, opts, cb) {
    if (!opts || !opts.preserveSymlinks) {
        realpath(x, cb);
    } else {
        cb(null, x);
    }
}

function defaultReadPackage(readFile, pkgfile, cb) {
    readFile(pkgfile, function (readFileErr, body) {
        if (readFileErr) cb(readFileErr);
        else {
            try {
                var pkg = JSON.parse(body);
                cb(null, pkg);
            } catch (jsonErr) {
                cb(jsonErr);
            }
        }
    });
}

function getPackageCandidates(x, start, opts) {
    var dirs = nodeModulesPaths(start, opts, x);
    for (var i = 0; i < dirs.length; i++) {
        dirs[i] = path.join(dirs[i], x);
    }
    return dirs;
}

function findUpConsumer(currentDir, isFile, readPackage, readFile, done) {
    var pkgfile = path.join(currentDir, 'package.json');
    isFile(pkgfile, function (err, exists) {
        if (err) { return done(err); }
        if (exists) {
            readPackage(readFile, pkgfile, function (readErr, pkg) {
                if (readErr && !(readErr instanceof SyntaxError)) { return done(readErr); }
                done(null, pkg || null);
            });
        } else {
            var parentDir = path.dirname(currentDir);
            if (parentDir === currentDir) {
                return done(null, null);
            }
            findUpConsumer(parentDir, isFile, readPackage, readFile, done);
        }
    });
}

function findConsumerPackage(startDir, isFile, readPackage, readFile, done) {
    var dir = path.resolve(startDir);
    findUpConsumer(dir, isFile, readPackage, readFile, done);
}

function findUpWithDir(currentDir, isFile, readPackage, readFile, done) {
    // Stop at node_modules boundaries - can't self-reference across node_modules
    if (nodeModulesRegex.test(currentDir)) {
        return done(null, null);
    }
    var pkgfile = path.join(currentDir, 'package.json');
    isFile(pkgfile, function (err, exists) {
        if (err) { return done(err); }
        if (exists) {
            readPackage(readFile, pkgfile, function (readErr, pkg) {
                if (readErr && !(readErr instanceof SyntaxError)) { return done(readErr); }
                done(null, pkg ? {
                    __proto__: null, pkg: pkg, dir: currentDir
                } : null);
            });
        } else {
            var parentDir = path.dirname(currentDir);
            if (parentDir === currentDir) {
                return done(null, null);
            }
            findUpWithDir(parentDir, isFile, readPackage, readFile, done);
        }
    });
}

module.exports = function resolve(x, options, callback) {
    var cb = callback;
    var opts = options;
    if (typeof options === 'function') {
        cb = opts;
        opts = {};
    }
    if (typeof x !== 'string') {
        var err = new $TypeError('Path must be a string.');
        return process.nextTick(function () {
            cb(err);
        });
    }

    opts = normalizeOptions(x, opts);

    var isFile = opts.isFile || defaultIsFile;
    var isDirectory = opts.isDirectory || defaultIsDir;
    var readFile = opts.readFile || fs.readFile;
    var realpath = opts.realpath || defaultRealpath;
    var readPackage = opts.readPackage || defaultReadPackage;
    if (opts.readFile && opts.readPackage) {
        var conflictErr = new $TypeError('`readFile` and `readPackage` are mutually exclusive.');
        return process.nextTick(function () {
            cb(conflictErr);
        });
    }
    var packageIterator = opts.packageIterator;

    if (typeof opts.moduleSystem !== 'undefined' && opts.moduleSystem !== 'require' && opts.moduleSystem !== 'import') {
        var msErr = new $TypeError('`moduleSystem` must be `\'require\'` or `\'import\'`.');
        return process.nextTick(function () {
            cb(msErr);
        });
    }

    var extensions = opts.extensions || ['.js'];
    var includeCoreModules = opts.includeCoreModules !== false;
    var basedir = opts.basedir || path.dirname(caller());
    var parent = opts.filename || basedir;

    opts.paths = opts.paths || defaultPaths();

    // Determine exports category
    var exportsCategory;
    // hoist the caught error into a `var` rather than closing over the catch binding;
    // old V8 (node < 4) drops the catch param when the inner closure is rewritten by nyc.
    var catErr;
    try {
        exportsCategory = getExportsCategory(opts);
    } catch (e) {
        catErr = e;
    }
    if (catErr) {
        return process.nextTick(function () {
            cb(catErr);
        });
    }

    // ensure that `basedir` is an absolute path at this point, resolving against the process' current working directory
    var absoluteStart = path.resolve(basedir);

    maybeRealpath(
        realpath,
        absoluteStart,
        opts,
        function (err, realStart) {
            if (err) cb(err);
            else if (exportsCategory === 'engines') {
                // Need to read consumer's package.json for engines.node
                findConsumerPackage(realStart, isFile, readPackage, readFile, function (findErr, consumerPkg) {
                    if (findErr) return cb(findErr);
                    if (consumerPkg && consumerPkg.engines && consumerPkg.engines.node) {
                        var categories = getCategoriesForRange(consumerPkg.engines.node);
                        exportsCategory = selectMostRestrictive(categories);
                    } else {
                        exportsCategory = null;
                    }
                    validateBasedir(realStart);
                });
            } else {
                validateBasedir(realStart);
            }
        }
    );

    function findPackageWithDir(startDir, done) {
        var dir = path.resolve(startDir);
        findUpWithDir(dir, isFile, readPackage, readFile, done);
    }

    function resolveSelfReference(x, startDir, done) {
        var parsed = parsePackageSpecifier(x);
        findPackageWithDir(startDir, function (err, pkgInfo) {
            if (err) return done(err);
            if (!pkgInfo || !pkgInfo.pkg || pkgInfo.pkg.name !== parsed.name) {
                return done(null, null); // Not a self-reference
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
                var resolved;
                try {
                    resolved = resolveExports(pkg.exports, parsed.subpath, conditions, categoryInfo.flags);
                } catch (exportsErr) {
                    return done(exportsErr);
                }
                if (resolved) {
                    var resolvedPath = path.resolve(pkgDir, resolved);
                    isFile(resolvedPath, function (err, exists) {
                        if (err) return done(err);
                        if (exists) return done(null, resolvedPath, pkg);
                        // File doesn't exist
                        done(null, undefined);
                    });
                    return;
                }
                // exports field exists but didn't resolve
                return done(null, undefined);
            }

            // No exports field - fall back to traditional resolution for self-reference
            if (parsed.subpath === '.') {
                loadAsDirectory(pkgDir, pkg, function (err, result, resultPkg) {
                    done(err, result, resultPkg);
                });
            } else {
                var subPath = path.join(pkgDir, parsed.subpath.slice(1));
                loadAsFile(subPath, pkg, function (err, m, mPkg) {
                    if (err) return done(err);
                    if (m) return done(null, m, mPkg);
                    loadAsDirectory(subPath, pkg, function (err, n, nPkg) {
                        done(err, n, nPkg);
                    });
                });
            }
        });
    }

    function validateBasedir(basedir) {
        if (opts.basedir) {
            var dirError = new $TypeError('Provided basedir "' + basedir + '" is not a directory' + (opts.preserveSymlinks ? '' : ', or a symlink to a directory'));
            dirError.code = 'INVALID_BASEDIR';
            isDirectory(basedir, function (err, result) {
                if (err) return cb(err);
                if (!result) { return cb(dirError); }
                validBasedir(basedir);
            });
        } else {
            validBasedir(basedir);
        }
    }

    var useExports = false;
    var res;
    function validBasedir(basedir) {
        useExports = exportsCategory !== null && exportsCategory !== 'pre-exports';

        if (relativePathRegex.test(x)) {
            res = path.resolve(basedir, x);
            if (x === '.' || x === '..' || x.slice(-1) === '/') res += '/';
            if (x.slice(-1) === '/' && res === basedir) {
                loadAsDirectory(res, opts.package, onfile);
            } else loadAsFile(res, opts.package, onfile);
        } else if (includeCoreModules && isCore(x)) {
            return cb(null, x);
        } else if (useExports) {
            // Try self-reference resolution first
            resolveSelfReference(x, basedir, function (selfErr, selfRef, selfPkg) {
                if (selfErr) return cb(selfErr);
                if (selfRef) {
                    return maybeRealpath(realpath, selfRef, opts, function (err, realSelf) {
                        if (err) cb(err);
                        else cb(null, realSelf, selfPkg);
                    });
                }
                if (selfRef === undefined) {
                    // exports field exists but didn't resolve - error per Node semantics
                    var moduleError = new $Error("Cannot find module '" + x + "' from '" + parent + "'");
                    moduleError.code = 'MODULE_NOT_FOUND';
                    return cb(moduleError);
                }
                loadNodeModulesWithExports(x, basedir, function (err, n, pkg) {
                    if (err) cb(err);
                    else if (n) {
                        return maybeRealpath(realpath, n, opts, function (err, realN) {
                            if (err) {
                                cb(err);
                            } else {
                                cb(null, realN, pkg);
                            }
                        });
                    } else {
                        var moduleError = new $Error("Cannot find module '" + x + "' from '" + parent + "'");
                        moduleError.code = 'MODULE_NOT_FOUND';
                        cb(moduleError);
                    }
                });
            });
        } else {
            loadNodeModules(x, basedir, function (err, n, pkg) {
                if (err) cb(err);
                else if (n) {
                    return maybeRealpath(realpath, n, opts, function (err, realN) {
                        if (err) {
                            cb(err);
                        } else {
                            cb(null, realN, pkg);
                        }
                    });
                } else {
                    var moduleError = new $Error("Cannot find module '" + x + "' from '" + parent + "'");
                    moduleError.code = 'MODULE_NOT_FOUND';
                    cb(moduleError);
                }
            });
        }
    }

    function onfile(err, m, pkg) {
        if (err) cb(err);
        else if (m) cb(null, m, pkg);
        else loadAsDirectory(res, function (err, d, pkg) {
            if (err) cb(err);
            else if (d) {
                maybeRealpath(realpath, d, opts, function (err, realD) {
                    if (err) {
                        cb(err);
                    } else {
                        cb(null, realD, pkg);
                    }
                });
            } else {
                var moduleError = new $Error("Cannot find module '" + x + "' from '" + parent + "'");
                moduleError.code = 'MODULE_NOT_FOUND';
                cb(moduleError);
            }
        });
    }

    function loadAsFile(x, thePackage, callback) {
        var loadAsFilePackage = thePackage;
        var cb = callback;
        if (typeof loadAsFilePackage === 'function') {
            cb = loadAsFilePackage;
            loadAsFilePackage = undefined;
        }

        var exts = [''].concat(extensions);
        load(exts, x, loadAsFilePackage);

        function load(exts, x, loadPackage) {
            if (exts.length === 0) return cb(null, undefined, loadPackage);
            var file = x + exts[0];

            var pkg = loadPackage;
            if (pkg) onpkg(null, pkg);
            else loadpkg(path.dirname(file), onpkg);

            function onpkg(err, pkg_, dir) {
                pkg = pkg_;
                if (err) return cb(err);
                if (dir && pkg && opts.pathFilter) {
                    var rfile = path.relative(dir, file);
                    var rel = rfile.slice(0, rfile.length - exts[0].length);
                    var r = opts.pathFilter(pkg, x, rel);
                    if (r) return load(
                        [''].concat(extensions),
                        path.resolve(dir, r),
                        pkg
                    );
                }
                isFile(file, onex);
            }
            function onex(err, ex) {
                if (err) return cb(err);
                if (ex) return cb(null, file, pkg);
                load(exts.slice(1), x, pkg);
            }
        }
    }

    function loadpkg(dir, cb) {
        if (dir === '' || dir === '/') return cb(null);
        if (process.platform === 'win32' && windowsDriveRegex.test(dir)) {
            return cb(null);
        }
        if (nodeModulesRegex.test(dir)) return cb(null);

        maybeRealpath(realpath, dir, opts, function (unwrapErr, pkgdir) {
            if (unwrapErr) return loadpkg(path.dirname(dir), cb);
            var pkgfile = path.join(pkgdir, 'package.json');
            isFile(pkgfile, function (err, ex) {
                // on err, ex is false
                if (!ex) return loadpkg(path.dirname(dir), cb);

                readPackage(readFile, pkgfile, function (err, pkgParam) {
                    if (err && !(err instanceof SyntaxError)) return cb(err);

                    var pkg = pkgParam;

                    if (pkg && opts.packageFilter) {
                        pkg = opts.packageFilter(pkg, pkgfile, dir);
                    }
                    cb(null, pkg, dir);
                });
            });
        });
    }

    function loadAsDirectory(x, loadAsDirectoryPackage, callback) {
        var cb = callback;
        var fpkg = loadAsDirectoryPackage;
        if (typeof fpkg === 'function') {
            cb = fpkg;
            fpkg = opts.package;
        }

        maybeRealpath(realpath, x, opts, function (unwrapErr, pkgdir) {
            if (unwrapErr) return loadAsDirectory(path.dirname(x), fpkg, cb);
            var pkgfile = path.join(pkgdir, 'package.json');
            isFile(pkgfile, function (err, ex) {
                if (err) return cb(err);
                if (!ex) return loadAsFile(path.join(x, 'index'), fpkg, cb);

                readPackage(readFile, pkgfile, function (err, pkgParam) {
                    if (err) return cb(err);

                    var pkg = pkgParam;

                    if (pkg && opts.packageFilter) {
                        pkg = opts.packageFilter(pkg, pkgfile, pkgdir);
                    }

                    if (pkg && pkg.main) {
                        if (typeof pkg.main !== 'string') {
                            var mainError = new $TypeError('package “' + pkg.name + '” `main` must be a string');
                            mainError.code = 'INVALID_PACKAGE_MAIN';
                            return cb(mainError);
                        }
                        if (pkg.main === '.' || pkg.main === './') {
                            pkg.main = 'index';
                        }
                        loadAsFile(path.resolve(x, pkg.main), pkg, function (err, m, pkg) {
                            if (err) return cb(err);
                            if (m) return cb(null, m, pkg);
                            if (!pkg) return loadAsFile(path.join(x, 'index'), pkg, cb);

                            var dir = path.resolve(x, pkg.main);
                            loadAsDirectory(dir, pkg, function (err, n, pkg) {
                                if (err) return cb(err);
                                if (n) return cb(null, n, pkg);
                                loadAsFile(path.join(x, 'index'), pkg, function (err, m, pkg) {
                                    if (err) return cb(err);
                                    if (m) return cb(null, m, pkg);
                                    var incorrectMainError = new $Error("Cannot find module '" + path.resolve(x, pkg.main) + "'. Please verify that the package.json has a valid \"main\" entry");
                                    incorrectMainError.code = 'INCORRECT_PACKAGE_MAIN';
                                    return cb(incorrectMainError);
                                });
                            });
                        });
                        return;
                    }

                    loadAsFile(path.join(x, '/index'), pkg, cb);
                });
            });
        });
    }

    function processDirs(cb, dirs) {
        if (dirs.length === 0) return cb(null, undefined);
        var dir = dirs[0];

        isDirectory(path.dirname(dir), isdir);

        function isdir(err, isdir) {
            if (err) return cb(err);
            if (!isdir) return processDirs(cb, dirs.slice(1));
            loadAsFile(dir, opts.package, onfile);
        }

        function onfile(err, m, pkg) {
            if (err) return cb(err);
            if (m) return cb(null, m, pkg);
            loadAsDirectory(dir, opts.package, ondir);
        }

        function ondir(err, n, pkg) {
            if (err) return cb(err);
            if (n) return cb(null, n, pkg);
            processDirs(cb, dirs.slice(1));
        }
    }
    function loadNodeModules(x, start, cb) {
        var thunk = function () { return getPackageCandidates(x, start, opts); };
        processDirs(
            cb,
            packageIterator ? packageIterator(x, start, thunk, opts) : thunk()
        );
    }

    function loadNodeModulesWithExports(x, start, done) {
        var parsed = parsePackageSpecifier(x);
        var categoryInfo = getCategoryInfo(exportsCategory, opts.moduleSystem || 'require');
        var conditions = opts.conditions || categoryInfo.conditions;

        var thunk = function () { return getPackageCandidates(parsed.name, start, opts); };
        var dirs = packageIterator ? packageIterator(parsed.name, start, thunk, opts) : thunk();

        processExportsDirs(dirs, 0);

        function processExportsDirs(dirs, idx) {
            if (idx >= dirs.length) return done(null, undefined);
            var pkgDir = dirs[idx];

            isDirectory(pkgDir, function (err, isDir) {
                if (err) return done(err);
                if (!isDir) return processExportsDirs(dirs, idx + 1);

                var pkgfile = path.join(pkgDir, 'package.json');
                isFile(pkgfile, function (err, exists) {
                    if (err) return done(err);
                    if (!exists) {
                        // No package.json, fall back to file/directory resolution
                        if (parsed.subpath === '.') {
                            loadAsFile(pkgDir, opts.package, function (err, m, pkg) {
                                if (err) return done(err);
                                if (m) return done(null, m, pkg);
                                loadAsDirectory(pkgDir, opts.package, function (err, n, pkg) {
                                    if (err) return done(err);
                                    if (n) return done(null, n, pkg);
                                    processExportsDirs(dirs, idx + 1);
                                });
                            });
                        } else {
                            var fullPath = path.join(pkgDir, parsed.subpath.slice(1));
                            loadAsFile(fullPath, opts.package, function (err, m, pkg) {
                                if (err) return done(err);
                                if (m) return done(null, m, pkg);
                                loadAsDirectory(fullPath, opts.package, function (err, n, pkg) {
                                    if (err) return done(err);
                                    if (n) return done(null, n, pkg);
                                    processExportsDirs(dirs, idx + 1);
                                });
                            });
                        }
                        return;
                    }

                    readPackage(readFile, pkgfile, function (err, pkg) {
                        if (err) {
                            if (!(err instanceof SyntaxError)) return done(err);
                            return processExportsDirs(dirs, idx + 1);
                        }

                        if (pkg && opts.packageFilter) {
                            // eslint-disable-next-line no-param-reassign
                            pkg = opts.packageFilter(pkg, pkgfile, pkgDir);
                        }

                        // If package has exports field, use exports resolution
                        if (pkg && typeof pkg.exports !== 'undefined') {
                            var resolved;
                            try {
                                resolved = resolveExports(pkg.exports, parsed.subpath, conditions, categoryInfo.flags);
                            } catch (exportsErr) {
                                return done(exportsErr);
                            }
                            if (resolved) {
                                var resolvedPath = path.resolve(pkgDir, resolved);
                                isFile(resolvedPath, function (err, exists) {
                                    if (err) return done(err);
                                    if (exists) return done(null, resolvedPath, pkg);
                                    // File doesn't exist
                                    done(null, undefined);
                                });
                                return;
                            }
                            // exports field exists but didn't resolve
                            return done(null, undefined);
                        }

                        // No exports field, fall back to traditional resolution
                        if (parsed.subpath === '.') {
                            loadAsDirectory(pkgDir, pkg, function (err, result, resultPkg) {
                                if (err) return done(err);
                                if (result) return done(null, result, resultPkg);
                                processExportsDirs(dirs, idx + 1);
                            });
                        } else {
                            var subPath = path.join(pkgDir, parsed.subpath.slice(1));
                            loadAsFile(subPath, pkg, function (err, m, mPkg) {
                                if (err) return done(err);
                                if (m) return done(null, m, mPkg);
                                loadAsDirectory(subPath, pkg, function (err, n, nPkg) {
                                    if (err) return done(err);
                                    if (n) return done(null, n, nPkg);
                                    processExportsDirs(dirs, idx + 1);
                                });
                            });
                        }
                    });
                });
            });
        }
    }
};
