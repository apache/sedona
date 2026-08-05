import type { Category, Condition } from './types';
import type getCategoryFlags from './getCategoryFlags';

declare namespace getCategoryInfo {
	interface CategoryInfo {
		conditions: Condition[] | null;
		flags: getCategoryFlags.CategoryFlags;
	}
}

declare function getCategoryInfo(
	category: Category,
	moduleSystem?: 'import' | 'require'
): getCategoryInfo.CategoryInfo;

export = getCategoryInfo;
