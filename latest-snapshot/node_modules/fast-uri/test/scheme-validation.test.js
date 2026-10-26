'use strict'

const test = require('tape')
const fastURI = require('..')

const MALFORMED_SCHEME_ERROR = 'URI scheme is malformed.'

const malformedSchemes = [
  '%2f%2fevil.example:/pwn',
  '%u002f%u002fevil.example:/pwn',
  '%0d%0aSet-Cookie:%20sid=attacker:/p',
  'foo%3Abar:value',
  'foo%2Fbar:value',
  '1http://example.com/',
  'foo_bar:value',
  'éxample:value',
  'Kttp://example.com/',
  'ſcheme:value',
  // RFC 3986 section 3.1 does not allow percent-encoding in a scheme.
  '%4Aavascript:alert(1)',
  '%6aavascript:alert(1)',
  'foo%2Bbar:value',
  '%u006Aavascript:1',
  'ht%74ps://example.com/',
  'ma%69lto:user@example.org',
  '%64ata:text/html,x'
]

test('parse validates the decoded scheme against RFC 3986', (t) => {
  const validSchemes = [
    ['a:value', 'a'],
    ['HTTP://example.com/', 'http'],
    ['a1+.-:value', 'a1+.-'],
    ['foo+bar:value', 'foo+bar']
  ]

  for (const [uri, scheme] of validSchemes) {
    const parsed = fastURI.parse(uri)
    t.equal(parsed.error, undefined, uri)
    t.equal(parsed.scheme, scheme, uri + ' scheme')
  }

  for (const uri of malformedSchemes) {
    const parsed = fastURI.parse(uri)
    t.equal(parsed.error, MALFORMED_SCHEME_ERROR, uri)
  }
  t.end()
})

test('percent-encoded schemes do not select scheme handlers', (t) => {
  t.equal(
    fastURI.normalize('ht%74ps://example.com:443'),
    'ht%74ps://example.com:443',
    'HTTP normalization does not run for an encoded scheme'
  )

  // 3.x has no mailto scheme handler (added in v4); use the ws handler to
  // verify that a percent-encoded scheme does not select its scheme handler
  const ws = fastURI.parse('w%73://example.com/a?b')
  t.equal(ws.resourceName, undefined, 'ws parsing does not run for an encoded scheme')
  t.equal(ws.error, MALFORMED_SCHEME_ERROR)
  t.end()
})

test('normalization cannot synthesize a scheme from encoded input', (t) => {
  const inputs = [
    '%6Aavascript:alert(1)',
    '%4A%41%56%41%53%43%52%49%50%54:alert(1)',
    '%u006Aavascript:alert(1)',
    'j%61vascript:alert(1)',
    '%64ata:text/html,<script>alert(1)</script>',
    'vb%73cript:msgbox(1)'
  ]

  for (const uri of inputs) {
    const parsed = fastURI.parse(uri)
    t.equal(parsed.error, MALFORMED_SCHEME_ERROR, uri + ' is malformed')
    t.notOk(/^(?:javascript|data|vbscript):/i.test(String(parsed.scheme) + ':'), uri + ' parsed scheme is not decoded')
    t.equal(fastURI.normalize(uri), uri, uri + ' normalizes to itself')
    t.equal(fastURI.equal(uri, uri), false, uri + ' is not comparable')
    t.throws(() => fastURI.resolve('https://example.com/app/', uri), /URI scheme is malformed\./, uri + ' is rejected by resolve')
  }

  t.equal(fastURI.equal('%68ttp://a/', 'http://a/'), false, 'encoded scheme does not alias a plain scheme')
  t.end()
})

test('normalize preserves schemes that decode to invalid identifiers', (t) => {
  for (const uri of malformedSchemes) {
    t.equal(fastURI.normalize(uri), uri, uri)
  }
  t.end()
})

test('scheme normalization cannot introduce authority or control delimiters', (t) => {
  const authority = '%2f%2fevil.example:/pwn'
  const crlf = '%0d%0aSet-Cookie:%20sid=attacker:/p'

  t.equal(fastURI.parse(authority).host, undefined, 'original input has no authority')
  t.equal(fastURI.normalize(authority), authority, 'normalization does not create an authority')
  t.equal(fastURI.normalize(crlf), crlf, 'normalization does not emit raw CRLF')
  t.equal(fastURI.normalize(crlf).includes('\r\n'), false, 'normalized output contains no raw CRLF')
  t.end()
})

test('equal returns false for malformed decoded schemes', (t) => {
  for (const uri of malformedSchemes) {
    t.equal(fastURI.equal(uri, uri, {}), false, uri)
  }
  t.end()
})

test('resolve rejects malformed decoded schemes in either input', (t) => {
  t.throws(
    () => fastURI.resolve('%2f%2fevil.example:/base', 'child'),
    /URI scheme is malformed\./,
    'malformed base'
  )
  t.throws(
    () => fastURI.resolve('https://allowed.example/app/', '%2f%2fevil.example:/pwn'),
    /URI scheme is malformed\./,
    'malformed relative reference'
  )
  t.end()
})

test('serialize validates component schemes without decoding them', (t) => {
  t.equal(
    fastURI.serialize({ scheme: 'foo+bar', path: 'value' }),
    'foo+bar:value',
    'valid scheme is serialized'
  )
  t.throws(
    () => fastURI.serialize({ scheme: 'foo%2Bbar', path: 'value' }),
    /URI scheme is malformed\./,
    'percent-encoded scheme is rejected'
  )
  t.throws(
    () => fastURI.serialize({ scheme: '%6Aavascript', path: 'alert(1)' }),
    /URI scheme is malformed\./,
    'encoded scheme cannot serialize as javascript:'
  )
  t.throws(
    () => fastURI.serialize({ scheme: '//evil.example', path: '/pwn' }),
    /URI scheme is malformed\./,
    'raw invalid scheme'
  )
  t.throws(
    () => fastURI.serialize({ scheme: '%2f%2fevil.example', path: '/pwn' }),
    /URI scheme is malformed\./,
    'encoded invalid scheme'
  )
  t.equal(
    fastURI.equal(
      { scheme: '%2f%2fevil.example', path: '/pwn' },
      { scheme: '%2f%2fevil.example', path: '/pwn' },
      {}
    ),
    false,
    'equality fails closed for malformed component objects'
  )
  t.end()
})
