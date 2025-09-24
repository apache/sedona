var test = require('tape')
var fs = require('vinyl-fs')
var through = require('through')
var named = require('../index')

test('add named to files', function(t) {
  t.plan(1)
  fs.src('test/fixtures/one.js')
    .pipe(named())
    .pipe(through(function(file) {
      t.equal(file.named, 'one', 'named should equal one')
    }, t.end))
})

test('add named with factory', function(t) {
  t.plan(1)
  fs.src('test/fixtures/one.js')
    .pipe(named(function(file) {
      return 'pineapple'
    }))
    .pipe(through(function(file) {
      t.equal(file.named, 'pineapple', 'named should equal pineapple')
    }, t.end))
})

test('add custom name key to files', function(t) {
  t.plan(1)
  fs.src('test/fixtures/one.js')
    .pipe(named(function(file) {
      file.customName = 'pineapple'
      this.queue(file)
    }))
    .pipe(through(function(file) {
      t.equal(file.customName, 'pineapple', 'customName should equal pineapple')
    }, t.end))
})
