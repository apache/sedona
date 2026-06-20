import ljharb from '@ljharb/eslint-config/flat';

export default [
	...ljharb,
	{
		rules: {
			eqeqeq: ['error', 'allow-null'],
			'max-lines-per-function': 'off',
			'new-cap': [
				'error', {
					capIsNewExceptions: [
						'HasOwnProperty',
						'IsCallable',
					],
				},
			],
			'no-extra-parens': 'off',
		},
	},
];
