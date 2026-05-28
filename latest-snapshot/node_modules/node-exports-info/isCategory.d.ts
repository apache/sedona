import type { Category } from './types';

declare function isCategory(category: unknown): category is Category;

export = isCategory;
