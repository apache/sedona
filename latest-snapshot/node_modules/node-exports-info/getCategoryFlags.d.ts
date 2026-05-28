import type { Category } from './types';

declare namespace getCategoryFlags {
	interface CategoryFlags {
		patterns: boolean;
		patternTrailers: boolean;
		dirSlash: boolean;
	}
}

declare function getCategoryFlags(category: Category): getCategoryFlags.CategoryFlags;

export = getCategoryFlags;
