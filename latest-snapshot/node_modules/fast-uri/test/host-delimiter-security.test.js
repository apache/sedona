'use strict'

const test = require('tape')
const fastURI = require('..')

const MALFORMED_HOST_ERROR = 'URI host is malformed.'

const domainHostSchemes = ['http', 'https', 'ws', 'wss']

// Hosts containing ASCII characters that RFC 3986 does not allow in a
// reg-name. Handing them to the WHATWG URL parser would read "@" as a userinfo
// delimiter (dropping the data before it) and strip TAB, LF and CR.
const forbiddenHosts = [
  'b@evil.example',
  'ev\til.example',
  'ev\nil.example',
  'ev\ril.example',
  'ev\r\nil.example',
  'a b.example',
  'a"b.example',
  'a<b.example',
  'a>b.example',
  'a^b.example',
  'a`b.example',
  'a{b.example',
  'a|b.example',
  'a}b.example',
  'a\u007Fb.example',
  'a\u0000b.example'
]

test('domain hosts with forbidden ASCII characters fail closed', (t) => {
  for (const scheme of domainHostSchemes) {
    for (const host of forbiddenHosts) {
      const uri = `${scheme}://user@${host}/path`
      const label = JSON.stringify(uri)

      const parsed = fastURI.parse(uri)
      t.equal(parsed.error, MALFORMED_HOST_ERROR, `${label} reports a malformed host`)
      t.notEqual(parsed.host, 'evil.example', `${label} does not report a truncated host`)

      t.equal(fastURI.normalize(uri), uri, `${label} normalizes to itself`)
      t.equal(fastURI.normalize(uri, { unicodeSupport: true }), uri, `${label} normalizes to itself with unicodeSupport`)
      t.equal(fastURI.equal(uri, uri), false, `${label} is not comparable`)
      t.equal(fastURI.equal(uri, `${scheme}://user@evil.example/path`), false, `${label} does not alias the stripped host`)

      t.throws(
        () => fastURI.resolve(`${scheme}://trusted.example/`, `//user@${host}/path`),
        /URI host is malformed\./,
        `${label} is rejected by resolve`
      )
    }
  }
  t.end()
})

test('a second "@" cannot move the authority to a different host', (t) => {
  const uri = 'http://a@b@evil.example/'
  const parsed = fastURI.parse(uri)
  t.equal(parsed.userinfo, 'a')
  t.notEqual(parsed.host, 'evil.example', 'host is not silently truncated')
  t.equal(parsed.error, MALFORMED_HOST_ERROR)
  t.equal(fastURI.normalize(uri), uri)

  t.throws(
    () => fastURI.resolve('http://trusted.example/', '//a@b%41@evil.example/p'),
    /URI host is malformed\./,
    'percent-encoded data does not bypass the check in resolve'
  )
  t.end()
})

test('the domainHost option applies the same rules to other schemes', (t) => {
  const parsed = fastURI.parse('foo://a@b@evil.example/', { domainHost: true })
  t.equal(parsed.error, MALFORMED_HOST_ERROR)
  t.end()
})

test('valid domain hosts are unaffected', (t) => {
  t.equal(fastURI.normalize('http://Example.COM/'), 'http://example.com/')
  t.equal(fastURI.normalize('http://%65xample.com/'), 'http://example.com/')
  t.equal(fastURI.normalize('http://bücher.de/'), 'http://xn--bcher-kva.de/')
  t.equal(fastURI.normalize('http://bücher.de/', { unicodeSupport: true }), 'http://bücher.de/')
  t.equal(fastURI.parse("http://a!$&'()*+,;=b.example/").error, undefined, 'sub-delims are allowed')
  t.equal(fastURI.parse('http://user:pass@example.com:8080/').host, 'example.com')
  t.equal(fastURI.resolve('http://trusted.example/', '//user@example.com/p'), 'http://user@example.com/p')
  t.end()
})

test('schemes without domain hosts keep encoding reg-name data', (t) => {
  t.equal(fastURI.normalize('foo://a@b@evil.example/'), 'foo://a@b%40evil.example/')
  t.equal(fastURI.resolve('foo://x/', '//a@b@evil.example/p'), 'foo://a@b%40evil.example/p')
  t.end()
})
