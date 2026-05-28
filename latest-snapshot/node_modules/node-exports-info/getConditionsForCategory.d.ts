import type { Category, Condition } from './types';

declare function getConditionsForCategory(
    category: Category,
    moduleSystem?: 'import' | 'require'
): (
    | ['default']
    | ['import', 'node', 'default']
    | ['node', 'require', 'default']
    | ['import', 'node', 'require', 'default']
    | ['import', 'node-addons', 'node', 'default']
    | ['node-addons', 'node', 'require', 'default']
    | ['import', 'node-addons', 'node', 'require', 'default']
    | ['import', 'node-addons', 'node', 'module-sync', 'default']
    | ['node-addons', 'node', 'require', 'module-sync', 'default']
    | ['import', 'node-addons', 'node', 'require', 'module-sync', 'default']
    | null
);

export = getConditionsForCategory;