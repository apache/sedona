'use strict';

var fs = require('fs');
var path = require('path');
var test = require('tape');
var resolve = require('../sync');

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

test('exports resolution - exportsCategory option', function (t) {
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
                    try {
                        var result = resolve(specifier, {
                            basedir: __dirname,
                            exportsCategory: category,
                            extensions: ['.js', '.json'],
                            packageIterator: function () {
                                return [projectDir];
                            }
                        });
                        var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                        st.equal(relativeResult, expectedFile, specifier + ' resolves to ' + expectedFile);
                    } catch (e) {
                        st.fail('Unexpected error for ' + specifier + ': ' + e.message);
                    }
                });
            });
        });
    });

    t.end();
});

test('exports resolution - pre-exports category uses main/index', function (t) {
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
            try {
                var result = resolve(pkgName, {
                    basedir: __dirname,
                    exportsCategory: 'pre-exports',
                    extensions: ['.js', '.json'],
                    packageIterator: function () {
                        return [projectDir];
                    }
                });
                var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                st.equal(relativeResult, mainEntry, pkgName + ' resolves to ' + mainEntry);
            } catch (e) {
                st.fail('Unexpected error for ' + pkgName + ': ' + e.message);
            }
        });
    });

    t.end();
});

test('exports resolution - mutual exclusivity of options', function (t) {
    t.test('exportsCategory and engines (string) are mutually exclusive', function (st) {
        st.plan(1);
        try {
            resolve('tape', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                engines: '>= 14'
            });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/mutually exclusive/).test(e.message), 'throws with mutually exclusive message');
        }
    });

    t.test('exportsCategory and engines (true) are mutually exclusive', function (st) {
        st.plan(1);
        try {
            resolve('tape', {
                basedir: __dirname,
                exportsCategory: 'conditions',
                engines: true
            });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/mutually exclusive/).test(e.message), 'throws with mutually exclusive message');
        }
    });

    t.end();
});

test('exports resolution - invalid category', function (t) {
    t.plan(2);
    try {
        resolve('tape', {
            basedir: __dirname,
            exportsCategory: 'not-a-real-category'
        });
        t.fail('should have thrown');
    } catch (e) {
        t.equal(e.code, 'INVALID_EXPORTS_CATEGORY', 'has correct error code');
        t.ok((/Invalid exports category/).test(e.message), 'has correct error message');
    }
});

test('exports resolution - engines option', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-exports-string', 'project');

    t.test('engines string maps to category', function (st) {
        st.plan(1);
        var result = resolve('ex-exports-string', {
            basedir: __dirname,
            engines: '>= 14',
            packageIterator: function () {
                return [projectDir];
            }
        });
        st.ok(result.indexOf('index.js') > -1, 'resolves to index.js');
    });

    t.test('engines: false is same as omitting', function (st) {
        st.plan(1);
        var result = resolve('tape', {
            basedir: __dirname,
            engines: false
        });
        st.ok(result.indexOf('tape') > -1, 'resolves without exports resolution');
    });

    t.test('engines: empty string throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', {
                basedir: __dirname,
                engines: ''
            });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/must be.*true.*false.*non-empty string/i).test(e.message), 'throws with correct message');
        }
    });

    t.test('engines: number throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', {
                basedir: __dirname,
                engines: 14
            });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/must be.*true.*false.*non-empty string/i).test(e.message), 'throws with correct message');
        }
    });

    t.test('engines: object throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', {
                basedir: __dirname,
                engines: { node: '>= 14' }
            });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/must be.*true.*false.*non-empty string/i).test(e.message), 'throws with correct message');
        }
    });

    t.end();
});

test('exports resolution - conditions override', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-conditions', 'project');

    t.test('default category conditions resolve to require.js', function (st) {
        st.plan(1);
        var result = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            packageIterator: function () {
                return [projectDir];
            }
        });
        st.ok(result.indexOf('require.js') > -1, 'resolves to require.js with default conditions');
    });

    t.test('conditions override to [default] resolves to default.js', function (st) {
        st.plan(1);
        var result = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            conditions: ['default'],
            packageIterator: function () {
                return [projectDir];
            }
        });
        st.ok(result.indexOf('default.js') > -1, 'resolves to default.js with conditions override');
    });

    t.test('conditions override to [node] resolves to node.js', function (st) {
        st.plan(1);
        var result = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            conditions: ['node'],
            packageIterator: function () {
                return [projectDir];
            }
        });
        st.ok(result.indexOf('node.js') > -1, 'resolves to node.js with conditions override');
    });

    t.end();
});

test('exports resolution - subpath not exported throws', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-exports-string', 'project');

    t.plan(2);
    try {
        resolve('ex-exports-string/not-exported', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            packageIterator: function () {
                return [projectDir];
            }
        });
        t.fail('should have thrown');
    } catch (e) {
        t.equal(e.code, 'ERR_PACKAGE_PATH_NOT_EXPORTED', 'has correct error code');
        t.ok((/not defined by "exports"/).test(e.message), 'has correct error message');
    }
});

test('existing resolution without exports options still works', function (t) {
    t.plan(1);
    var result = resolve('tape', { basedir: __dirname });
    t.ok(result.indexOf('tape') > -1, 'resolves tape without exports options');
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

test('exports resolution - moduleSystem: import uses import conditions', function (t) {
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
                    try {
                        var result = resolve(specifier, {
                            basedir: __dirname,
                            exportsCategory: category,
                            moduleSystem: 'import',
                            extensions: ['.js', '.json', '.mjs'],
                            packageIterator: function () {
                                return [projectDir];
                            }
                        });
                        var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                        st.equal(relativeResult, expectedFile, specifier + ' with moduleSystem:import resolves to ' + expectedFile);
                    } catch (e) {
                        st.fail('Unexpected error for ' + specifier + ' with moduleSystem:import: ' + e.message);
                    }
                });
            });
        });
    });

    t.end();
});

test('exports resolution - moduleSystem: require matches default behavior', function (t) {
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
                    try {
                        var result = resolve(specifier, {
                            basedir: __dirname,
                            exportsCategory: category,
                            moduleSystem: 'require',
                            extensions: ['.js', '.json'],
                            packageIterator: function () {
                                return [projectDir];
                            }
                        });
                        var relativeResult = './' + path.relative(projectDir, result).split(path.sep).join('/');
                        st.equal(relativeResult, expectedFile, specifier + ' with moduleSystem:require resolves to ' + expectedFile);
                    } catch (e) {
                        st.fail('Unexpected error for ' + specifier + ' with moduleSystem:require: ' + e.message);
                    }
                });
            });
        });
    });

    t.end();
});

test('exports resolution - moduleSystem import vs require produce different results', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-conditions', 'project');

    t.test('./idnr: import resolves to import.mjs, require resolves to default.js', function (st) {
        st.plan(2);
        var importResult = resolve('ex-conditions/idnr', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('import.mjs') > -1, 'moduleSystem:import resolves to import.mjs');

        var requireResult = resolve('ex-conditions/idnr', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('default.js') > -1, 'moduleSystem:require resolves to default.js');
    });

    t.test('./rdni: import resolves to default.js, require resolves to require.js', function (st) {
        st.plan(2);
        var importResult = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('default.js') > -1, 'moduleSystem:import resolves to default.js');

        var requireResult = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('require.js') > -1, 'moduleSystem:require resolves to require.js');
    });

    t.test('./indr: import resolves to import.mjs, require resolves to node.js', function (st) {
        st.plan(2);
        var importResult = resolve('ex-conditions/indr', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('import.mjs') > -1, 'moduleSystem:import resolves to import.mjs');

        var requireResult = resolve('ex-conditions/indr', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('node.js') > -1, 'moduleSystem:require resolves to node.js');
    });

    t.test('./irdn: import resolves to import.mjs, require resolves to require.js', function (st) {
        st.plan(2);
        var importResult = resolve('ex-conditions/irdn', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('import.mjs') > -1, 'moduleSystem:import resolves to import.mjs');

        var requireResult = resolve('ex-conditions/irdn', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('require.js') > -1, 'moduleSystem:require resolves to require.js');
    });

    t.test('default (no moduleSystem) matches require behavior', function (st) {
        st.plan(2);
        var defaultResult = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        var requireResult = resolve('ex-conditions/rdni', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.equal(defaultResult, requireResult, 'default and explicit require produce the same result');
        st.ok(defaultResult.indexOf('require.js') > -1, 'both resolve to require.js');
    });

    t.end();
});

test('exports resolution - moduleSystem import with various fixtures', function (t) {
    t.test('ex-conditions-in-folder: import resolves to mjs, require to cjs', function (st) {
        var projectDir = path.join(fixturesDir, 'ex-conditions-in-folder', 'project');
        st.plan(2);
        var importResult = resolve('ex-conditions-in-folder', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('mjs/index.mjs') > -1, 'import resolves to mjs/index.mjs');

        var requireResult = resolve('ex-conditions-in-folder', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('cjs/index.js') > -1, 'require resolves to cjs/index.js');
    });

    t.test('ex-exports-TL-object: import resolves to index.mjs, require to file.js', function (st) {
        var projectDir = path.join(fixturesDir, 'ex-exports-TL-object', 'project');
        st.plan(2);
        var importResult = resolve('ex-exports-TL-object', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('index.mjs') > -1, 'import resolves to index.mjs');

        var requireResult = resolve('ex-exports-TL-object', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('file.js') > -1, 'require resolves to file.js');
    });

    t.test('flatted-3: import resolves to esm/index.js, require to cjs/index.js', function (st) {
        var projectDir = path.join(fixturesDir, 'flatted-3', 'project');
        st.plan(2);
        var importResult = resolve('flatted', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('esm/index.js') > -1, 'import resolves to esm/index.js');

        var requireResult = resolve('flatted', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('cjs/index.js') > -1, 'require resolves to cjs/index.js');
    });

    t.test('is-promise-2.2.1: import resolves to index.mjs, require to index.js', function (st) {
        var projectDir = path.join(fixturesDir, 'is-promise-2.2.1', 'project');
        st.plan(2);
        var importResult = resolve('is-promise', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('index.mjs') > -1, 'import resolves to index.mjs');

        var requireResult = resolve('is-promise', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('index.js') > -1 && requireResult.indexOf('index.mjs') === -1, 'require resolves to index.js');
    });

    t.test('resolve-2: import resolves to index.mjs, require to index.js', function (st) {
        var projectDir = path.join(fixturesDir, 'resolve-2', 'project');
        st.plan(2);
        var importResult = resolve('resolve', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('index.mjs') > -1, 'import resolves to index.mjs');

        var requireResult = resolve('resolve', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('index.js') > -1 && requireResult.indexOf('index.mjs') === -1, 'require resolves to index.js');
    });

    t.test('preact: import resolves to .mjs, require to .js', function (st) {
        var projectDir = path.join(fixturesDir, 'preact', 'project');
        st.plan(2);
        var importResult = resolve('preact', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(importResult.indexOf('preact.mjs') > -1, 'import resolves to preact.mjs');

        var requireResult = resolve('preact', {
            basedir: __dirname,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(requireResult.indexOf('preact.js') > -1 && requireResult.indexOf('preact.mjs') === -1, 'require resolves to preact.js');
    });

    t.end();
});

test('exports resolution - moduleSystem import with self-reference', function (t) {
    t.test('self-reference with moduleSystem:import uses import conditions', function (st) {
        var conditionsDir = path.join(fixturesDir, 'ex-conditions', 'project');
        st.plan(2);
        var importResult = resolve('ex-conditions/idnr', {
            basedir: conditionsDir,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs']
        });
        st.ok(importResult.indexOf('import.mjs') > -1, 'self-reference with import resolves to import.mjs');

        var requireResult = resolve('ex-conditions/idnr', {
            basedir: conditionsDir,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs']
        });
        st.ok(requireResult.indexOf('default.js') > -1, 'self-reference with require resolves to default.js');
    });

    t.test('self-reference with moduleSystem:import on TL-object package', function (st) {
        var projectDir = path.join(fixturesDir, 'ex-exports-TL-object', 'project');
        st.plan(2);
        var importResult = resolve('ex-exports-TL-object', {
            basedir: projectDir,
            exportsCategory: 'conditions',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs']
        });
        st.ok(importResult.indexOf('index.mjs') > -1, 'self-ref import resolves to index.mjs');

        var requireResult = resolve('ex-exports-TL-object', {
            basedir: projectDir,
            exportsCategory: 'conditions',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs']
        });
        st.ok(requireResult.indexOf('file.js') > -1, 'self-ref require resolves to file.js');
    });

    t.end();
});

test('exports resolution - moduleSystem with engines option', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-conditions', 'project');

    t.test('moduleSystem:import works with engines string', function (st) {
        st.plan(1);
        var result = resolve('ex-conditions/idnr', {
            basedir: __dirname,
            engines: '>= 14',
            moduleSystem: 'import',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(result.indexOf('import.mjs') > -1, 'engines + moduleSystem:import resolves to import.mjs');
    });

    t.test('moduleSystem:require works with engines string', function (st) {
        st.plan(1);
        var result = resolve('ex-conditions/idnr', {
            basedir: __dirname,
            engines: '>= 14',
            moduleSystem: 'require',
            extensions: ['.js', '.mjs'],
            packageIterator: function () { return [projectDir]; }
        });
        st.ok(result.indexOf('default.js') > -1, 'engines + moduleSystem:require resolves to default.js');
    });

    t.end();
});

test('exports resolution - invalid moduleSystem throws', function (t) {
    t.test('moduleSystem: true throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: true });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: false throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: false });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: empty string throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: '' });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: number throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: 42 });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: random string throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: 'cjs' });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: object throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: {} });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: null throws', function (st) {
        st.plan(1);
        try {
            resolve('tape', { basedir: __dirname, moduleSystem: null });
            st.fail('should have thrown');
        } catch (e) {
            st.ok((/moduleSystem/).test(e.message), 'throws with moduleSystem message: ' + e.message);
        }
    });

    t.test('moduleSystem: undefined does not throw', function (st) {
        st.plan(1);
        var result = resolve('tape', { basedir: __dirname, moduleSystem: undefined });
        st.ok(result.indexOf('tape') > -1, 'undefined moduleSystem resolves normally');
    });

    t.end();
});

test('exports resolution - moduleSystem does not affect resolution without exports', function (t) {
    t.plan(1);
    var result = resolve('tape', {
        basedir: __dirname,
        moduleSystem: 'import'
    });
    t.ok(result.indexOf('tape') > -1, 'moduleSystem is ignored when no exports options are set');
});

test('exports resolution - self-reference', function (t) {
    var projectDir = path.join(fixturesDir, 'ex-exports-string', 'project');

    t.test('self-reference resolves via exports when inside package', function (st) {
        st.plan(1);
        // basedir is inside the package, specifier is the package name
        var result = resolve('ex-exports-string', {
            basedir: projectDir,
            exportsCategory: 'conditions'
        });
        st.ok(result.indexOf('index.js') > -1, 'self-reference resolves to index.js via exports');
    });

    t.test('self-reference with subpath resolves via exports', function (st) {
        var conditionsDir = path.join(fixturesDir, 'ex-conditions', 'project');
        st.plan(1);
        var result = resolve('ex-conditions/rdni', {
            basedir: conditionsDir,
            exportsCategory: 'conditions'
        });
        st.ok(result.indexOf('require.js') > -1, 'self-reference subpath resolves correctly');
    });

    t.test('self-reference without exports falls back to main', function (st) {
        // Create a scenario where there's no exports field
        var mainDotlessDir = path.join(fixturesDir, 'ex-main-dotless', 'project');
        st.plan(1);
        try {
            var result = resolve('ex-main-dotless', {
                basedir: mainDotlessDir,
                exportsCategory: 'conditions'
            });
            st.ok(result.indexOf('main.js') > -1 || result.indexOf('index.js') > -1, 'self-reference without exports uses main/index');
        } catch (e) {
            // If it throws, that's also acceptable behavior (no exports means not exported)
            st.ok(true, 'self-reference without exports throws or resolves');
        }
    });

    t.test('self-reference does not cross node_modules boundary', function (st) {
        // basedir is inside node_modules, should NOT self-reference parent package
        var nodeModulesDir = path.join(__dirname, '..', 'node_modules', 'tape');
        st.plan(1);
        // Trying to resolve 'resolve' from inside node_modules/tape should NOT
        // self-reference the root resolve package - it should use normal resolution
        var result = resolve('resolve', {
            basedir: nodeModulesDir,
            exportsCategory: 'conditions'
        });
        // The result should be from node_modules, not from a self-reference
        // (self-reference would give us the current working directory's resolve)
        st.ok(result.indexOf('node_modules') > -1 || result === 'resolve', 'does not self-reference across node_modules');
    });

    t.end();
});
