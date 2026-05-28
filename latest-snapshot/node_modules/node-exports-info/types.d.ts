import ranges from './ranges';

export type Category = typeof ranges[Exclude<keyof typeof ranges, '__proto__'>];

export type Range = Exclude<keyof typeof ranges, '__proto__'>;

export type RangePair = [Range, Category];

export type Condition = 'node' | 'node-addons' | 'import' | 'require' | 'module-sync' | 'default';