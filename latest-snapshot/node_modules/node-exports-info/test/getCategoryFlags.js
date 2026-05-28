'use strict';

var test = require('tape');
var forEach = require('for-each');

var getCategoryFlags = require('../getCategoryFlags');
var getRangePairs = require('../getRangePairs');

test('getCategoryFlags', function (t) {
	t['throws'](
		// @ts-expect-error
		function () { getCategoryFlags('not a category'); },
		RangeError,
		'invalid category throws'
	);

	forEach(getRangePairs(), function (pair) {
		var category = pair[1];
		t.test('category: ' + category, function (st) {
			var flags = getCategoryFlags(category);

			st.ok(
				flags && typeof flags === 'object',
				'returns an object'
			);
			st.ok(
				'patterns' in flags && typeof flags.patterns === 'boolean',
				'has boolean patterns flag'
			);
			st.ok(
				'patternTrailers' in flags && typeof flags.patternTrailers === 'boolean',
				'has boolean patternTrailers flag'
			);
			st.ok(
				'dirSlash' in flags && typeof flags.dirSlash === 'boolean',
				'has boolean dirSlash flag'
			);

			// Verify flag consistency: patternTrailers implies patterns
			if (flags.patternTrailers) {
				st.ok(flags.patterns, 'patternTrailers implies patterns');
			}

			st.end();
		});
	});

	t.test('specific category flags', function (st) {
		st.deepEqual(
			getCategoryFlags('pre-exports'),
			{ patterns: false, patternTrailers: false, dirSlash: false },
			'pre-exports has no flags'
		);

		st.deepEqual(
			getCategoryFlags('broken'),
			{ patterns: false, patternTrailers: false, dirSlash: false },
			'broken has no flags'
		);

		st.deepEqual(
			getCategoryFlags('experimental'),
			{ patterns: false, patternTrailers: false, dirSlash: false },
			'experimental has no flags'
		);

		st.deepEqual(
			getCategoryFlags('conditions'),
			{ patterns: false, patternTrailers: false, dirSlash: false },
			'conditions has no flags'
		);

		st.deepEqual(
			getCategoryFlags('broken-dir-slash-conditions'),
			{ patterns: false, patternTrailers: false, dirSlash: true },
			'broken-dir-slash-conditions has dirSlash'
		);

		st.deepEqual(
			getCategoryFlags('patterns'),
			{ patterns: true, patternTrailers: false, dirSlash: true },
			'patterns has patterns and dirSlash'
		);

		st.deepEqual(
			getCategoryFlags('pattern-trailers'),
			{ patterns: true, patternTrailers: true, dirSlash: true },
			'pattern-trailers has all flags'
		);

		st.deepEqual(
			getCategoryFlags('pattern-trailers+json-imports'),
			{ patterns: true, patternTrailers: true, dirSlash: true },
			'pattern-trailers+json-imports has all flags'
		);

		st.deepEqual(
			getCategoryFlags('pattern-trailers-no-dir-slash'),
			{ patterns: true, patternTrailers: true, dirSlash: false },
			'pattern-trailers-no-dir-slash has patterns and patternTrailers but not dirSlash'
		);

		st.deepEqual(
			getCategoryFlags('pattern-trailers-no-dir-slash+json-imports'),
			{ patterns: true, patternTrailers: true, dirSlash: false },
			'pattern-trailers-no-dir-slash+json-imports has patterns and patternTrailers but not dirSlash'
		);

		st.deepEqual(
			getCategoryFlags('require-esm'),
			{ patterns: true, patternTrailers: true, dirSlash: false },
			'require-esm has patterns and patternTrailers but not dirSlash'
		);

		st.deepEqual(
			getCategoryFlags('strips-types'),
			{ patterns: true, patternTrailers: true, dirSlash: false },
			'strips-types has patterns and patternTrailers but not dirSlash'
		);

		st.deepEqual(
			getCategoryFlags('subpath-imports-slash'),
			{ patterns: true, patternTrailers: true, dirSlash: true },
			'subpath-imports-slash has all flags'
		);

		st.end();
	});

	t.end();
});
