import ljharbConfig from '@ljharb/eslint-config/flat';

export default [
	...ljharbConfig,
	{
		rules: {
			'func-name-matching': ['error', 'always'],
			'id-length': 'off',
			'multiline-comment-style': 'off',
			'no-magic-numbers': ['error', { ignore: [0, 1] }],
			'sort-keys': 'off',
		},
	},
];
