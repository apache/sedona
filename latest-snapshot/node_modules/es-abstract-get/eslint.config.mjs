import ljharb from '@ljharb/eslint-config/flat';
import ljharbTests from '@ljharb/eslint-config/flat/tests';

export default [
	{
		ignores: [
			'coverage',
			'.nyc_output',
		],
	},
	...ljharb,
	{
		rules: {
			eqeqeq: ['error', 'allow-null'],
			'func-style': ['error', 'declaration'],
			'id-length': [
				'error', {
					max: 24,
					min: 1,
					properties: 'never',
				},
			],
			'multiline-comment-style': 'off',
			'new-cap': [
				'error', {
					capIsNewExceptions: [
						'Get',
						'GetMethod',
						'GetV',
						'ToObject',
					],
				},
			],
			'no-extra-parens': 'off',
			'no-magic-numbers': 'off',
		},
	},
	...ljharbTests.map((c) => ({
		...c,
		files: ['test/**'],
	})),
];
