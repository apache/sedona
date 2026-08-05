import path from 'path';
import expect from 'expect.js';
import vinyl from 'vinyl-fs';
import sassGlob from '../src';

describe('gulp-sass-glob', () => {
  it('(scss) should parse a single directory AND support single and double quotes @import usage', (done) => {
    const expectedResult = [
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/single-directory.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(sass) should parse a single directory', (done) => {
    const expectedResult = [
      '@import "import/_f1.scss"',
      '@import "import/_f2.scss"'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/single-directory.sass'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(sass) should understand imports with fixed file name', (done) => {
    const expectedResult = [
      '@import "recursive/nested/_f3.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/fixed-name.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) should parse a directory recursively', (done) => {
    const expectedResult = [
      '@import "recursive/_f1.scss";',
      '@import "recursive/_f2.scss";',
      '@import "recursive/nested/_f3.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/recursive.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) should find multiple imports', (done) => {
    const expectedResult = [
      '@import "recursive/_f1.scss";',
      '@import "recursive/_f2.scss";',
      '@import "recursive/nested/_f3.scss";',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/multiple.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) should omit ignored directories', (done) => {
    const expectedResult = [
      '@import "recursive/_f1.scss";',
      '@import "recursive/_f2.scss";',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/multiple.scss'))
      .pipe(sassGlob({
        ignorePaths: [
          'recursive/nested/**'
        ]
      }))
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) should allow globbing when ignoring files', (done) => {
    const expectedResult = [
      '@import "recursive/_f2.scss";',
      '@import "recursive/nested/_f3.scss";',
      '@import "import/_f2.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/multiple.scss'))
      .pipe(sassGlob({
        ignorePaths: [
          '**/_f1.scss'
        ]
      }))
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) should allow multiple ignore patterns', (done) => {
    const expectedResult = [
      '@import "recursive/nested/_f3.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/multiple.scss'))
      .pipe(sassGlob({
        ignorePaths: [
          '**/_f1.scss',
          'recursive/_f2.scss',
          'import/**'
        ]
      }))
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) should ignore commented globs', (done) => {
    vinyl
      .src(path.join(__dirname, '/test-scss/ignore-comments.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(contents);
      })
      .on('end', done);
  });

  it('(scss) should ignore empty directories', (done) => {
    const expectedResult = [
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/ignore-empty.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });
  it('(scss) should use includePaths like gulp-sass', (done) => {
    const expectedResult = [
      '@import "nested/_f3.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/option-includePaths.scss'))
      .pipe(sassGlob({
        includePaths: [
          path.join(__dirname, '/test-scss/recursive/')
        ]
      }))
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });
  it('(scss) should use includePaths priority, first relative file and position in includePath', (done) => {
    const expectedResult = [
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      '@import "nested/_f3.scss";'
    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/option-includePaths-priority.scss'))
      .pipe(sassGlob({
        includePaths: [
          path.join(__dirname, '/test-scss/recursive/'),
          path.join(__dirname, '/test-scss/includePaths/')
        ]
      }))
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });

  it('(scss) Issue #28', (done) => {
    const expectedResult = [
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      ' // must_be_fix, this comment stripped',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      ' /* Start multiline comment',
      '*/',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      ' /*',
      'Start 2 multiline comment',
      '*/',
      '/* And this */',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      '/* begin comment */',
      '@import "import/_f1.scss";',
      '@import "import/_f2.scss";',
      ' /* end comment */',
      '// @import "import/*" - ignore this',
      '// end //'

    ].join('\n');

    vinyl
      .src(path.join(__dirname, '/test-scss/issue_28.scss'))
      .pipe(sassGlob())
      .on('data', (file) => {
        const contents = file.contents.toString('utf-8').trim();
        expect(contents).to.equal(expectedResult.trim());
      })
      .on('end', done);
  });
});
