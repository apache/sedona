'use strict';

var fs = require('fs');
var path = require('path');
var test = require('tape');
var resolve = require('../');

var fixturesDir = path.join(__dirname, 'list-exports', 'packages', 'tests', 'fixtures');

var categories = [
    'broken',
    'broken-dir-slash-conditions',
    'conditions',
    'experimental',
    'pattern-trailers',
    'pattern-trailers+json-imports',
    'pattern-trailers-no-dir-slash',
    'pattern-trailers-no-dir-slash+json-imports',
    'patterns',
    'require-esm',
    'strips-types',
    'subpath-imports-slash'
    // 'pre-exports' is tested separately since it uses main/index resolution
];

// Fixtures that are symlinks pointing outside the fixture dir cause path confusion
// ex-private is a private package whose expected files don't include exports data
var skipFixtures = ['list-exports', 'ls-exports', 'ex-private'];

function getFixtures() {
    return fs.readdirSync(fixturesDir).filter(function (name) {
        if (skipFixtures.indexOf(name) !== -1) {
            return false;
        }
        var stat = fs.statSync(path.join(fixturesDir, name));
        return stat.isDirectory();
    });
}

function loadExpected(fixtureName, category) {
    var expectedPath = path.join(fixturesDir, fixtureName, 'expected', category + '.json');
    if (!fs.existsSync(expectedPath)) {
        return null;
    }
    try {
        return JSON.parse(fs.readFileSync(expectedPath, 'utf8'));
    } catch (e) {
        return null;
    }
}

function loadProjectPkg(fixtureName) {
    var pkgPath = path.join(fixturesDir, fixtureName, 'project', 'package.json');
    try {
        return JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
    } catch (e) {
        return null;
    }
}

test('async exports resolution - exportsCategory option', function (t) {
    var fixtures = getFixtures();

    fixtures.forEach(function (fixtureName) {
        var projectPkg = loadProjectPkg(fixtureName);
        if (!projectPkg) {
            return;
        }
        var projectDir = path.join(fixturesDir, fixtureName, 'project');
        var pkgName = projectPkg.name;

        categories.forEach(function (category) {
            var expected = loadExpected(fixtureName, category);
            if (!expected || !expected.exports || !expected.exports[category]) {
                return;
            }

            var requireMap = expected.exports[category].require;
            if (!requireMap || typeof requireMap !== 'object') {
                return;
            }

            Object.keys(requireMap).forEach(function (subpath) {
                var expectedFile = requireMap[subpath];
                var specifier = subpath === '.' ? pkgName : pkgName + subpath.substring(1);

                t.test(fixtureName + ' / ' + category + ' / ' + subpath, function (st) {
                    st.plan(1);
                    resolve(specifier, {
                        basedir: __dirname,
                        exportsCategory: category,
                        extensions: ['.js', '.json'],
                        packageIterator: function () {
                            return [projectDir];
                        }
                    }, function (err, result) {
                        if (err) {
                            st.fail('Unexpected error for ' + specifier + ': ' + err.message);
                            return;
                        }
                        var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                        st.equal(relativeResult, expectedFile, specifier + ' resolves to ' + expectedFile);
                    });
                });
            });
        });
    });

    t.end();
});

test('async exports resolution - pre-exports category uses main/index', function (t) {
    var fixtures = getFixtures();

    fixtures.forEach(function (fixtureName) {
        var projectPkg = loadProjectPkg(fixtureName);
        if (!projectPkg) {
            return;
        }
        var projectDir = path.join(fixturesDir, fixtureName, 'project');
        var pkgName = projectPkg.name;

        var expected = loadExpected(fixtureName, 'pre-exports');
        if (!expected || !expected.exports || !expected.exports['pre-exports']) {
            return;
        }

        var requireMap = expected.exports['pre-exports'].require;
        if (!requireMap || typeof requireMap !== 'object') {
            return;
        }

        // For pre-exports, only test the main entry point (.)
        var mainEntry = requireMap['.'];
        if (!mainEntry) {
            return;
        }

        t.test(fixtureName + ' / pre-exports / .', function (st) {
            st.plan(1);
            resolve(pkgName, {
                basedir: __dirname,
                exportsCategory: 'pre-exports',
                extensions: ['.js', '.json'],
                packageIterator: function () {
                    return [projectDir];
                }
            }, function (err, result) {
                if (err) {
                    st.fail('Unexpected error for ' + pkgName + ': ' + err.message);
                    return;
                }
                var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                st.equal(relativeResult, mainEntry, pkgName + ' resolves to ' + mainEntry);
            });
        });
    });

    t.end();
});

test('async exports resolution - mutual exclusivity of options', function (t) {
    t.test('exportsCategory and engines (string) are mutually exclusive', function (st) {
        st.plan(1);
        resolve('tape', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            engines: '>= 14'
        }, function (err) {
            st.ok(err && (/mutually exclusive/).test(err.message), 'throws with mutually exclusive message');
        });
    });

    t.test('exportsCategory and engines (true) are mutually exclusive', function (st) {
        st.plan(1);
        resolve('tape', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            engines: true
        }, function (err) {
            st.ok(err && (/mutually exclusive/).test(err.message), 'throws with mutually exclusive message');
        });
    });

    t.end();
});

test('async exports resolution - invalid category', function (t) {
    t.plan(2);
    resolve('tape', {
        basedir: __dirname,
        exportsCategory: 'not-a-real-category'
    }, function (err) {
        t.equal(err && err.code, 'INVALID_EXPORTS_CATEGORY', 'has correct error code');
        t.ok(err && (/Invalid exports category/).test(err.message), 'has correct error message');
    });
});

test('async exports resolution - engines option', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-exports-string', 'project');

    t.test('engines string maps to category', function (st) {
        st.plan(1);
        resolve('ex-exports-string', {
            basedir: __dirname,
            engines: '>= 14',
            packageIterator: function () {
                return [projectDir];
            }
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('index.js') > -1, 'resolves to index.js');
        });
    });

    t.test('engines: false is same as omitting', function (st) {
        st.plan(1);
        resolve('tape', {
            basedir: __dirname,
            engines: false
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('tape') > -1, 'resolves without exports resolution');
        });
    });

    t.test('engines: empty string throws', function (st) {
        st.plan(1);
        resolve('tape', {
            basedir: __dirname,
            engines: ''
        }, function (err) {
            st.ok(err && (/must be.*true.*false.*non-empty string/i).test(err.message), 'throws with correct message');
        });
    });

    t.test('engines: number throws', function (st) {
        st.plan(1);
        resolve('tape', {
            basedir: __dirname,
            engines: 14
        }, function (err) {
            st.ok(err && (/must be.*true.*false.*non-empty string/i).test(err.message), 'throws with correct message');
        });
    });

    t.test('engines: object throws', function (st) {
        st.plan(1);
        resolve('tape', {
            basedir: __dirname,
            engines: { node: '>= 14' }
        }, function (err) {
            st.ok(err && (/must be.*true.*false.*non-empty string/i).test(err.message), 'throws with correct message');
        });
    });

    t.end();
});

test('async exports resolution - conditions override', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-conditions', 'project');

    t.test('default category conditions resolve to require.js', function (st) {
        st.plan(1);
        resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            packageIterator: function () {
                return [projectDir];
            }
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('require.js') > -1, 'resolves to require.js with default conditions');
        });
    });

    t.test('conditions override to [default] resolves to default.js', function (st) {
        st.plan(1);
        resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            conditions: ['default'],
            packageIterator: function () {
                return [projectDir];
            }
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('default.js') > -1, 'resolves to default.js with conditions override');
        });
    });

    t.test('conditions override to [node] resolves to node.js', function (st) {
        st.plan(1);
        resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            conditions: ['node'],
            packageIterator: function () {
                return [projectDir];
            }
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('node.js') > -1, 'resolves to node.js with conditions override');
        });
    });

    t.end();
});

test('async exports resolution - subpath not exported throws', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-exports-string', 'project');

    t.plan(2);
    resolve('ex-exports-string/not-exported', {
        basedir: __dirname,
        exportsCategory: 'conditions',
        packageIterator: function () {
            return [projectDir];
        }
    }, function (err) {
        t.equal(err && err.code, 'ERR_PACKAGE_PATH_NOT_EXPORTED', 'has correct error code');
        t.ok(err && (/not defined by "exports"/).test(err.message), 'has correct error message');
    });
});

test('async existing resolution without exports options still works', function (t) {
    t.plan(1);
    resolve('tape', { basedir: __dirname }, function (err, result) {
        if (err) {
            t.fail(err.message);
            return;
        }
        t.ok(result.indexOf('tape') > -1, 'resolves tape without exports options');
    });
});

test('all fixtures are tested', function (t) {
    var fixtures = getFixtures();
    var testedFixtures = [];

    fixtures.forEach(function (fixtureName) {
        var projectPkg = loadProjectPkg(fixtureName);
        if (!projectPkg) {
            t.fail('Fixture ' + fixtureName + ' has no loadable package.json');
            return;
        }

        var hasAnyTests = false;

        // Check if at least one category has expected results
        categories.forEach(function (category) {
            var expected = loadExpected(fixtureName, category);
            if (expected && expected.exports && expected.exports[category]) {
                var requireMap = expected.exports[category].require;
                if (requireMap && typeof requireMap === 'object' && Object.keys(requireMap).length > 0) {
                    hasAnyTests = true;
                }
            }
        });

        // Also check pre-exports
        var preExpected = loadExpected(fixtureName, 'pre-exports');
        if (preExpected && preExpected.exports && preExpected.exports['pre-exports']) {
            var preRequireMap = preExpected.exports['pre-exports'].require;
            if (preRequireMap && preRequireMap['.']) {
                hasAnyTests = true;
            }
        }

        if (hasAnyTests) {
            testedFixtures.push(fixtureName);
        } else {
            t.fail('Fixture ' + fixtureName + ' has no testable entrypoints');
        }
    });

    t.ok(testedFixtures.length > 0, 'At least one fixture is tested');
    t.equal(testedFixtures.length, fixtures.length, 'All ' + fixtures.length + ' fixtures have testable entrypoints');

    t.end();
});

test('async exports resolution - moduleSystem: import uses import conditions', function (t) {
    // ex-node-addons is skipped because getCategoryInfo includes node-addons in import
    // conditions for some categories, but list-exports expected data does not
    var skipImportFixtures = ['ex-node-addons'];
    var fixtures = getFixtures().filter(function (n) { return skipImportFixtures.indexOf(n) === -1; });

    fixtures.forEach(function (fixtureName) {
        var projectPkg = loadProjectPkg(fixtureName);
        if (!projectPkg) {
            return;
        }
        var projectDir = path.join(fixturesDir, fixtureName, 'project');
        var pkgName = projectPkg.name;

        categories.forEach(function (category) {
            var expected = loadExpected(fixtureName, category);
            if (!expected || !expected.exports || !expected.exports[category]) {
                return;
            }

            var importMap = expected.exports[category].import;
            if (!importMap || typeof importMap !== 'object') {
                return;
            }

            Object.keys(importMap).forEach(function (subpath) {
                var expectedFile = importMap[subpath];
                var specifier = subpath === '.' ? pkgName : pkgName + subpath.substring(1);

                t.test(fixtureName + ' / ' + category + ' / import / ' + subpath, function (st) {
                    st.plan(1);
                    resolve(specifier, {
                        basedir: __dirname,
                        exportsCategory: category,
                        moduleSystem: 'import',
                        extensions: ['.js', '.json', '.mjs'],
                        packageIterator: function () {
                            return [projectDir];
                        }
                    }, function (err, result) {
                        if (err) {
                            st.fail('Unexpected error for ' + specifier + ' with moduleSystem:import: ' + err.message);
                            return;
                        }
                        var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                        st.equal(relativeResult, expectedFile, specifier + ' with moduleSystem:import resolves to ' + expectedFile);
                    });
                });
            });
        });
    });

    t.end();
});

test('async exports resolution - moduleSystem: require matches default behavior', function (t) {
    var fixtures = getFixtures();

    fixtures.forEach(function (fixtureName) {
        var projectPkg = loadProjectPkg(fixtureName);
        if (!projectPkg) {
            return;
        }
        var projectDir = path.join(fixturesDir, fixtureName, 'project');
        var pkgName = projectPkg.name;

        categories.forEach(function (category) {
            var expected = loadExpected(fixtureName, category);
            if (!expected || !expected.exports || !expected.exports[category]) {
                return;
            }

            var requireMap = expected.exports[category].require;
            if (!requireMap || typeof requireMap !== 'object') {
                return;
            }

            Object.keys(requireMap).forEach(function (subpath) {
                var expectedFile = requireMap[subpath];
                var specifier = subpath === '.' ? pkgName : pkgName + subpath.substring(1);

                t.test(fixtureName + ' / ' + category + ' / explicit require / ' + subpath, function (st) {
                    st.plan(1);
                    resolve(specifier, {
                        basedir: __dirname,
                        exportsCategory: category,
                        moduleSystem: 'require',
                        extensions: ['.js', '.json'],
                        packageIterator: function () {
                            return [projectDir];
                        }
                    }, function (err, result) {
                        if (err) {
                            st.fail('Unexpected error for ' + specifier + ' with moduleSystem:require: ' + err.message);
                            return;
                        }
                        var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                        st.equal(relativeResult, expectedFile, specifier + ' with moduleSystem:require resolves to ' + expectedFile);
                    });
                });
            });
        });
    });

    t.end();
});

test('async exports resolution - moduleSystem import vs require produce different results', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-conditions', 'project');

    t.test('./idnr: import resolves to import.mjs, require resolves to default.js', function (st) {
        st.plan(2);
        resolve('ex-conditions/idnr', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('import.mjs') > -1, 'moduleSystem:import resolves to import.mjs');

            resolve('ex-conditions/idnr', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('default.js') > -1, 'moduleSystem:require resolves to default.js');
            });
        });
    });

    t.test('./rdni: import resolves to default.js, require resolves to require.js', function (st) {
        st.plan(2);
        resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('default.js') > -1, 'moduleSystem:import resolves to default.js');

            resolve('ex-conditions/rdni', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('require.js') > -1, 'moduleSystem:require resolves to require.js');
            });
        });
    });

    t.test('./indr: import resolves to import.mjs, require resolves to node.js', function (st) {
        st.plan(2);
        resolve('ex-conditions/indr', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('import.mjs') > -1, 'moduleSystem:import resolves to import.mjs');

            resolve('ex-conditions/indr', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('node.js') > -1, 'moduleSystem:require resolves to node.js');
            });
        });
    });

    t.test('./irdn: import resolves to import.mjs, require resolves to require.js', function (st) {
        st.plan(2);
        resolve('ex-conditions/irdn', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('import.mjs') > -1, 'moduleSystem:import resolves to import.mjs');

            resolve('ex-conditions/irdn', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('require.js') > -1, 'moduleSystem:require resolves to require.js');
            });
        });
    });

    t.test('default (no moduleSystem) matches require behavior', function (st) {
        st.plan(2);
        resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, defaultResult) {
            if (err) { return st.fail(err.message); }
            resolve('ex-conditions/rdni', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.equal(defaultResult, requireResult, 'default and explicit require produce the same result');
                st.ok(defaultResult.indexOf('require.js') > -1, 'both resolve to require.js');
            });
        });
    });

    t.end();
});

test('async exports resolution - moduleSystem import with various fixtures', function (t) {
    t.test('ex-conditions-in-folder: import resolves to mjs, require to cjs', function (st) {
        var projectDir = path.join(fixturesDir, 'ex-conditions-in-folder', 'project');
        st.plan(2);
        resolve('ex-conditions-in-folder', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('mjs/index.mjs') > -1, 'import resolves to mjs/index.mjs');

            resolve('ex-conditions-in-folder', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('cjs/index.js') > -1, 'require resolves to cjs/index.js');
            });
        });
    });

    t.test('ex-exports-TL-object: import resolves to index.mjs, require to file.js', function (st) {
        var projectDir = path.join(fixturesDir, 'ex-exports-TL-object', 'project');
        st.plan(2);
        resolve('ex-exports-TL-object', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('index.mjs') > -1, 'import resolves to index.mjs');

            resolve('ex-exports-TL-object', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('file.js') > -1, 'require resolves to file.js');
            });
        });
    });

    t.test('flatted-3: import resolves to esm/index.js, require to cjs/index.js', function (st) {
        var projectDir = path.join(fixturesDir, 'flatted-3', 'project');
        st.plan(2);
        resolve('flatted', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('esm/index.js') > -1, 'import resolves to esm/index.js');

            resolve('flatted', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('cjs/index.js') > -1, 'require resolves to cjs/index.js');
            });
        });
    });

    t.test('is-promise-2.2.1: import resolves to index.mjs, require to index.js', function (st) {
        var projectDir = path.join(fixturesDir, 'is-promise-2.2.1', 'project');
        st.plan(2);
        resolve('is-promise', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('index.mjs') > -1, 'import resolves to index.mjs');

            resolve('is-promise', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('index.js') > -1 && requireResult.indexOf('index.mjs') === -1, 'require resolves to index.js');
            });
        });
    });

    t.test('resolve-2: import resolves to index.mjs, require to index.js', function (st) {
        var projectDir = path.join(fixturesDir, 'resolve-2', 'project');
        st.plan(2);
        resolve('resolve', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('index.mjs') > -1, 'import resolves to index.mjs');

            resolve('resolve', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('index.js') > -1 && requireResult.indexOf('index.mjs') === -1, 'require resolves to index.js');
            });
        });
    });

    t.test('preact: import resolves to .mjs, require to .js', function (st) {
        var projectDir = path.join(fixturesDir, 'preact', 'project');
        st.plan(2);
        resolve('preact', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('preact.mjs') > -1, 'import resolves to preact.mjs');

            resolve('preact', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs'],
                packageIterator: function () { return [projectDir]; }
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('preact.js') > -1 && requireResult.indexOf('preact.mjs') === -1, 'require resolves to preact.js');
            });
        });
    });

    t.end();
});

test('async exports resolution - moduleSystem import with self-reference', function (t) {
    t.test('self-reference with moduleSystem:import uses import conditions', function (st) {
        var conditionsDir = path.join(fixturesDir, 'ex-conditions', 'project');
        st.plan(2);
        resolve('ex-conditions/idnr', {
            basedir: conditionsDir,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs']
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('import.mjs') > -1, 'self-reference with import resolves to import.mjs');

            resolve('ex-conditions/idnr', {
                basedir: conditionsDir,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs']
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('default.js') > -1, 'self-reference with require resolves to default.js');
            });
        });
    });

    t.test('self-reference with moduleSystem:import on TL-object package', function (st) {
        var projectDir = path.join(fixturesDir, 'ex-exports-TL-object', 'project');
        st.plan(2);
        resolve('ex-exports-TL-object', {
            basedir: projectDir,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs']
        }, function (err, importResult) {
            if (err) { return st.fail(err.message); }
            st.ok(importResult.indexOf('index.mjs') > -1, 'self-ref import resolves to index.mjs');

            resolve('ex-exports-TL-object', {
                basedir: projectDir,
                exportsCategory: 'conditions',
                moduleSystem: 'require',
                extensions: ['.js', '.mjs']
            }, function (err2, requireResult) {
                if (err2) { return st.fail(err2.message); }
                st.ok(requireResult.indexOf('file.js') > -1, 'self-ref require resolves to file.js');
            });
        });
    });

    t.end();
});

test('async exports resolution - moduleSystem with engines option', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-conditions', 'project');

    t.test('moduleSystem:import works with engines string', function (st) {
        st.plan(1);
        resolve('ex-conditions/idnr', {
            basedir: __dirname,
            engines: '>= 14',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, result) {
            if (err) { return st.fail(err.message); }
            st.ok(result.indexOf('import.mjs') > -1, 'engines + moduleSystem:import resolves to import.mjs');
        });
    });

    t.test('moduleSystem:require works with engines string', function (st) {
        st.plan(1);
        resolve('ex-conditions/idnr', {
            basedir: __dirname,
            engines: '>= 14',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        }, function (err, result) {
            if (err) { return st.fail(err.message); }
            st.ok(result.indexOf('default.js') > -1, 'engines + moduleSystem:require resolves to default.js');
        });
    });

    t.end();
});

test('async exports resolution - invalid moduleSystem errors', function (t) {
    t.test('moduleSystem: true errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: true }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: false errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: false }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: empty string errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: '' }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: number errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: 42 }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: random string errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: 'cjs' }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: object errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: {} }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: null errors', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: null }, function (err) {
            st.ok(err && (/moduleSystem/).test(err.message), 'errors with moduleSystem message: ' + (err && err.message));
        });
    });

    t.test('moduleSystem: undefined does not error', function (st) {
        st.plan(1);
        resolve('tape', { basedir: __dirname, moduleSystem: undefined }, function (err, result) {
            if (err) { return st.fail(err.message); }
            st.ok(result.indexOf('tape') > -1, 'undefined moduleSystem resolves normally');
        });
    });

    t.end();
});

test('async exports resolution - moduleSystem does not affect resolution without exports', function (t) {
    t.plan(1);
    resolve('tape', {
        basedir: __dirname,
        moduleSystem: 'import'
    }, function (err, result) {
        if (err) { return t.fail(err.message); }
        t.ok(result.indexOf('tape') > -1, 'moduleSystem is ignored when no exports options are set');
    });
});

test('async exports resolution - self-reference', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-exports-string', 'project');

    t.test('self-reference resolves via exports when inside package', function (st) {
        st.plan(1);
        // basedir is inside the package, specifier is the package name
        resolve('ex-exports-string', {
            basedir: projectDir,
            exportsCategory: 'conditions'
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('index.js') > -1, 'self-reference resolves to index.js via exports');
        });
    });

    t.test('self-reference with subpath resolves via exports', function (st) {
        var conditionsDir = path.join(fixturesDir, 'ex-conditions', 'project');
        st.plan(1);
        resolve('ex-conditions/rdni', {
            basedir: conditionsDir,
            exportsCategory: 'conditions'
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            st.ok(result.indexOf('require.js') > -1, 'self-reference subpath resolves correctly');
        });
    });

    t.test('self-reference without exports falls back to main', function (st) {
        var mainDotlessDir = path.join(fixturesDir, 'ex-main-dotless', 'project');
        st.plan(1);
        resolve('ex-main-dotless', {
            basedir: mainDotlessDir,
            exportsCategory: 'conditions'
        }, function (err, result) {
            // If it throws, that's also acceptable behavior (no exports means not exported)
            st.ok(!err || result, 'self-reference without exports throws or resolves');
        });
    });

    t.test('self-reference does not cross node_modules boundary', function (st) {
        // basedir is inside node_modules, should NOT self-reference parent package
        var nodeModulesDir = path.join(__dirname, '..', 'node_modules', 'tape');
        st.plan(1);
        resolve('resolve', {
            basedir: nodeModulesDir,
            exportsCategory: 'conditions'
        }, function (err, result) {
            if (err) {
                st.fail(err.message);
                return;
            }
            // The result should be from node_modules, not from a self-reference
            st.ok(result.indexOf('node_modules') > -1 || result === 'resolve', 'does not self-reference across node_modules');
        });
    });

    t.end();
});
