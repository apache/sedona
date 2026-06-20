import ljharbConfig from '@ljharb/eslint-config/flat';

export default [
	...ljharbConfig,
	{
		rules: {
			complexity: ['error', 11],
			'max-statements': ['error', 15],
			'new-cap': ['error', { capIsNewExceptions: ['IsCallable'] }],
		},
	},
];
