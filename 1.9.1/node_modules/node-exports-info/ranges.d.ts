declare const ranges: {
	__proto__: null;
	'>= 25.4': 'subpath-imports-slash';
	'23.6 - 25.3 || ^22.18': 'strips-types';
	'23 - 23.5 || 22.12 - 22.17 || ^20.19': 'require-esm';
	'17.1 - 19 || 20 - 20.18 || ^21 || 22 - 22.11': 'pattern-trailers-no-dir-slash+json-imports';
	'17.0': 'pattern-trailers-no-dir-slash';
	'^16.14': 'pattern-trailers+json-imports';
	'^14.19 || 16.9 - 16.13': 'pattern-trailers';
	'^12.20 || 14.13 - 14.18 || 15.x || 16.0 - 16.8': 'patterns';
	'12.17 - 12.19 || ^13.13 || 14.0 - 14.12': 'broken-dir-slash-conditions';
	'13.7 - 13.12': 'conditions';
	'13.3 - 13.6': 'experimental';
	'13.0 - 13.2': 'broken';
	'< 12.17': 'pre-exports';
};

export = ranges;
