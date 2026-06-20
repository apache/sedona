import ljharb from '@ljharb/eslint-config/flat';

export default [
	...ljharb,
	{
		rules: {
			'func-style': [
				'error',
				'declaration',
			],
			'id-length': [
				'error', {
					max: 24,
					min: 1,
					properties: 'never',
				},
			],
			'new-cap': [
				'error', {
					capIsNewExceptions: [
						'GetMethod',
						'OrdinaryToPrimitive',
					],
				},
			],
			'no-extra-parens': 'off',
		},
	},
];
