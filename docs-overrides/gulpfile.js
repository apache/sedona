'use strict';

let isProd = false;

const gulp = require('gulp'),
  gulpIf = require('gulp-if'),
  sass = require('gulp-sass')(require('sass')),
  autoprefixer = require('autoprefixer'),
  sassGlob = require('gulp-sass-glob'),
  uglify = require('gulp-uglify'),
  notify = require('gulp-notify'),
  plumber = require('gulp-plumber'),
  postcss = require('gulp-postcss'),
  rename = require('gulp-rename'),
  sourcemaps = require('gulp-sourcemaps'),
  rimraf = require('gulp-rimraf'),
  webpack = require('webpack-stream'),
  replace = require('gulp-replace'),
  named = require('vinyl-named'),
  through2 = require('through2'),
  path = require('path'),
  glob = require('glob'),
  browserSync = require('browser-sync').create();

gulp.task('public-js', function () {
  return gulp
    .src('assets/javascripts/main.js')
    .pipe(
      plumber(
        notify.onError({
          title: 'JS',
          message: 'Error: <%= error.message %>',
        }),
      ),
    )
    .pipe(
      webpack({
        mode: isProd ? 'production' : 'development',
        output: {
          filename: 'main.js',
          path: __dirname + '/assets/javascripts/',
        },
        externals: {
          jquery: 'jQuery',
        },
      }),
    )
    .pipe(
      rename(function (path) {
        path.basename += '.min';
      }),
    )
    .pipe(uglify())
    .pipe(gulp.dest('./assets/javascripts/'));
});

/* SCSS Tasks */
gulp.task('scss', function () {
  return gulp
    .src(['assets/stylesheets/*.scss', 'assets/stylesheets/**/.scss'])
    .pipe(gulpIf(!isProd, sourcemaps.init()))
    .pipe(sassGlob())
    .pipe(
      sass
        .sync({
          sourceMap: false,
          outputStyle: 'compressed',
          precision: 5,
          includePaths: ['node_modules/'],
        })
        .on('error', sass.logError),
    )
    .pipe(postcss([autoprefixer]))
    .pipe(rename({suffix: '.min'}))
    .pipe(gulpIf(!isProd, sourcemaps.write()))
    .pipe(gulpIf(isProd, sourcemaps.write('.')))
    .pipe(gulp.dest('assets/stylesheets/'))
    .pipe(browserSync.stream());
});

gulp.task('actual-watch', function () {
  // Watch SCSS files
  gulp.watch('assets/stylesheets/**/*.scss', gulp.series('scss'));

  // Watch Public JS files
  gulp.watch(
    ['assets/javascripts/**/*.js', '!assets/javascripts/**/*.min.js'],
    gulp.series('public-js'),
  );
});

/* Build dev */
gulp.task('build-dev', gulp.series('scss', 'public-js'));

/* Build to prod task */
gulp.task('build', gulp.series('scss', 'public-js'));

/* Watch task */
gulp.task('watch', gulp.series('build-dev', 'actual-watch'));
gulp.task('default', gulp.parallel('watch'));
