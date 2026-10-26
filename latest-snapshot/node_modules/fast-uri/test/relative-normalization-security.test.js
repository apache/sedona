'use strict'

const test = require('tape')
const fastURI = require('..')

const ABSOLUTE_BASES = [
  'http://h/p/q/r',
  'http://h/',
  'foo://h/a/b/',
  'http://h/p/q/'
]

const RELATIVE_BASES = ['', 'base', 'relative/base']

test('normalize keeps the meaning of relative-path references', (t) => {
  const cases = [
    ['a/../b', 'b'],
    ['x/..', './'],
    ['../x', '../x'],
    ['./../x', '../x'],
    ['a/b/../../..', '..'],
    ['a/./b', 'a/b'],
    ['a/b/..', 'a/'],
    ['.', './'],
    ['..', '..'],
    ['../', '../'],
    ['../../a/./b/../c', '../../a/c'],
    ['a//..', 'a/'],
    ['a/../b?q#f', 'b?q#f']
  ]
  for (const [input, expected] of cases) {
    t.equal(fastURI.normalize(input), expected, `${input} -> ${expected}`)
  }
  t.end()
})

test('normalize never turns a relative-path reference into a root or network path', (t) => {
  t.equal(fastURI.normalize('.//x'), './/x', 'leading empty segment is kept behind "."')
  t.equal(fastURI.normalize('a/..//x'), './/x', 'collapsed prefix does not expose "//"')
  t.equal(fastURI.parse(fastURI.normalize('a/..//x')).host, undefined, 'no authority is introduced')
  t.equal(fastURI.normalize('./a:b'), './a:b', 'first-segment colon is kept behind a "." segment (RFC 3986 section 4.2)')
  t.equal(fastURI.parse(fastURI.normalize('./a:b')).scheme, undefined, 'no scheme is introduced')
  t.end()
})

test('normalize keeps the existing behavior for root-relative and absolute paths', (t) => {
  t.equal(fastURI.normalize('/a/../b'), '/b')
  t.equal(fastURI.normalize('/../x'), '/x')
  t.equal(fastURI.normalize('http://h/a/../../b'), 'http://h/b')
  t.end()
})

test('equal does not alias relative references with different targets', (t) => {
  t.equal(fastURI.equal('../x', 'x'), false)
  t.equal(fastURI.equal('a/../b', '/b'), false)
  t.equal(fastURI.equal('x/..', '/'), false)
  t.equal(fastURI.equal('a/../b', 'b'), true)
  t.end()
})

test('resolving a normalized relative reference matches resolving the original', (t) => {
  for (const base of [...ABSOLUTE_BASES, ...RELATIVE_BASES]) {
    for (const ref of ['a/../b', 'x/..', '../x', './../x', 'a/b/../../..', '.', '..', './/x']) {
      t.equal(
        fastURI.resolve(base, fastURI.normalize(ref)),
        fastURI.resolve(base, ref),
        `${base} + ${ref}`
      )
    }
  }
  t.equal(fastURI.resolve('http://h/p/q/r', fastURI.normalize('a/../b')), 'http://h/p/q/b')
  t.equal(fastURI.resolve('http://h/p/q/r', fastURI.normalize('../x')), 'http://h/p/x')
  t.end()
})

test('property: normalization of relative references is resolution-preserving and idempotent', (t) => {
  const segments = ['a', 'b', '.', '..', '', 'a:b']
  // Deterministic linear congruential generator so failures are reproducible.
  let seed = 0x5ec02
  const next = (n) => {
    seed = (seed * 1103515245 + 12345) % 2147483648
    return Math.floor((seed / 2147483648) * n)
  }

  let failures = 0
  let checked = 0
  for (let i = 0; i < 4000; i++) {
    const length = 1 + next(6)
    const parts = []
    for (let j = 0; j < length; j++) parts.push(segments[next(segments.length)])
    const ref = parts.join('/')
    // Only relative-path references: empty refs, refs starting with "/", and
    // refs whose first segment contains ":" are not affected by this change.
    if (ref === '' || ref[0] === '/' || fastURI.parse(ref).scheme !== undefined) continue

    checked++
    const normalized = fastURI.normalize(ref)
    if (fastURI.normalize(normalized) !== normalized) {
      failures++
      t.fail(`not idempotent: ${ref} -> ${normalized} -> ${fastURI.normalize(normalized)}`)
    }
    // 3.x's removeDotSegments (used by resolve) collapses a trailing "//" to
    // "/" (fixed in v4 by #185), so resolve() itself is inconsistent for
    // these paths regardless of normalization.
    if (normalized.endsWith('//')) continue
    for (const base of ABSOLUTE_BASES) {
      const expected = fastURI.resolve(base, ref)
      const actual = fastURI.resolve(base, normalized)
      if (actual !== expected) {
        failures++
        t.fail(`${base} + ${ref}: normalized ${normalized} resolves to ${actual}, expected ${expected}`)
      }
    }
  }

  t.ok(checked > 1000, `checked ${checked} relative references`)
  t.equal(failures, 0, 'no relative reference changed meaning')
  t.end()
})

test('suffix normalization preserves relative path meaning', (t) => {
  const options = { reference: 'suffix', scheme: 'x' }
  for (const [input, expected] of [
    ['a/../b', 'b'],
    ['x/..', './'],
    ['a/../a:b', './a:b']
  ]) {
    t.equal(fastURI.normalize(input, options), expected, `${input} -> ${expected}`)
  }
  t.end()
})

test('normalize does not promote a colon-first relative reference to a scheme (skipEscape)', (t) => {
  // RFC 3986 section 4.2: a first path segment containing ":" must stay
  // protected by a leading "./", or it is mistaken for a scheme. Collapsing
  // the protecting dot segment must not expose the colon. The default path
  // escapes the colon; with skipEscape the colon is emitted raw and the
  // reference is reparsed as absolute (scheme "a"), changing its meaning.
  const colonRefs = ['./a:b', 'a/../a:b']
  for (const ref of colonRefs) {
    const normalized = fastURI.normalize(ref, { skipEscape: true })
    t.equal(
      fastURI.parse(normalized).scheme,
      undefined,
      `${ref} -> ${normalized} must not introduce a scheme`
    )
    for (const base of ABSOLUTE_BASES) {
      t.equal(
        fastURI.resolve(base, normalized, { skipEscape: true }),
        fastURI.resolve(base, ref, { skipEscape: true }),
        `${base} + ${ref}: normalized ${normalized} must resolve the same`
      )
    }
  }
  t.end()
})
