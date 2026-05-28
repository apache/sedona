import ljharbConfig from '@ljharb/eslint-config/flat';
import importPlugin from 'eslint-plugin-import';

export default [
	...ljharbConfig,
	{
		plugins: {
			import: importPlugin,
		},
		settings: {
			'import/resolver': {
				node: {
					extensions: ['.mjs', '.js', '.json'],
				},
			},
			'import/extensions': [
				'.js',
				'.mjs',
				'.jsx',
			],
			'import/core-modules': [
			],
			'import/ignore': [
				'node_modules',
			],
		},

		rules: {
			'import/no-unresolved': ['error', { commonjs: true, caseSensitive: true }],

			'import/named': 'error',
			'import/export': 'error',
			'import/no-named-as-default': 'error',
			'import/no-named-as-default-member': 'error',
			// https://github.com/benmosher/eslint-plugin-import/blob/master/docs/rules/no-deprecated.md
			'import/no-deprecated': 'error',

			'import/no-extraneous-dependencies': ['error', {
				devDependencies: [
					'eslint.config.mjs',
					'test/**',
				],
				optionalDependencies: false,
			}],
			'import/no-mutable-exports': 'error',
			'import/no-commonjs': 'off',
			'import/no-amd': 'error',
			'import/first': 'error',
			'import/no-duplicates': 'error',
			'import/no-namespace': 'error',

			// Ensure consistent use of file extension within the import path
			// https://github.com/benmosher/eslint-plugin-import/blob/master/docs/rules/extensions.md
			'import/extensions': ['error', 'ignorePackages', {
				json: 'never',
				js: 'never',
				mjs: 'never',
				jsx: 'never',
			}],
			'import/order': ['warn', { groups: [['builtin', 'external', 'internal']] }],
			'import/newline-after-import': 'error',
			'import/prefer-default-export': 'error',
			'import/no-restricted-paths': 'off',
			'import/max-dependencies': ['off', { max: 10 }],
			'import/no-absolute-path': 'error',
			'import/no-dynamic-require': 'error',
			'import/no-internal-modules': ['off', {
				allow: [],
			}],
			'import/no-webpack-loader-syntax': 'error',
			'import/no-unassigned-import': 'off',
			'import/no-named-default': 'error',
			'import/no-anonymous-default-export': ['off', {
				allowArray: false,
				allowArrowFunction: false,
				allowAnonymousClass: false,
				allowAnonymousFunction: false,
				allowLiteral: false,
				allowObject: false,
			}],
			'import/exports-last': 'error',
			'import/group-exports': 'off',
			'import/no-default-export': 'off',
			'import/no-named-export': 'off',
			'import/no-self-import': 'error',
			'import/no-cycle': ['error', { commonjs: true, maxDepth: '∞' }],
			'import/no-useless-path-segments': ['error', { commonjs: true }],
			'import/no-relative-parent-imports': 'off',
			// TODO: enable once it works for flat config
			'import/no-unused-modules': ['off', {
				commonjs: true,
				ignoreExports: [],
				missingExports: true,
				unusedExports: true,
			}],
			'import/no-import-module-exports': ['error', {
				exceptions: [],
			}],
			'import/no-relative-packages': 'error',
		},
	},
	{
		languageOptions: {
			globals: {
				Iterator: false,
			},
		},
		rules: {
			'array-bracket-newline': 'off',
			'func-name-matching': 'off',
			'id-length': 'off',
			'max-lines-per-function': 'off',
			'max-statements': 'off',
			'multiline-comment-style': 'off',
			'new-cap': ['error', {
				capIsNewExceptions: [
					'Call',
					'CreateDataPropertyOrThrow',
					'CreateIteratorFromClosure',
					'CreateIteratorResultObject',
					'GeneratorResume',
					'GeneratorResumeAbrupt',
					'GeneratorValidate',
					'Get',
					'GetIntrinsic',
					'GetIterator',
					'GetIteratorDirect',
					'GetIteratorFlattenable',
					'GetMethod',
					'GetOptionsObject',
					'IfAbruptCloseIterator',
					'IfAbruptCloseIterators',
					'IsAccessorDescriptor',
					'IsArray',
					'IsCallable',
					'IsDataDescriptor',
					'IteratorClose',
					'IteratorCloseAll',
					'IteratorStep',
					'IteratorStepValue',
					'IteratorZip',
					'NormalCompletion',
					'OrdinaryHasInstance',
					'OrdinaryObjectCreate',
					'ReturnCompletion',
					'SameValueZero',
					'StringToCodePoints',
					'ThrowCompletion',
					'ToBoolean',
					'ToIntegerOrInfinity',
					'ToNumber',
					'ToPropertyDescriptor',
					'ToString',
				],
			}],
			'no-negated-condition': 'warn',
			'object-curly-newline': 'off',
			'sort-keys': 'off',
		},
	},
	{
		files: ['test/**'],
		rules: {
			eqeqeq: ['error', 'allow-null'],
			'func-style': 'off',
		},
	},
	{
		files: ['Iterator.zip*/implementation.js'],
		rules: {
			complexity: 'off',
			'max-depth': 'off',
		},
	},
	{
		files: ['aos/IteratorZip.js'],
		rules: {
			'max-depth': 'off',
			'max-params': 'off',
		},
	},
];
