import type { default as resolve } from './index.js';

/** Asynchronously resolve a module path, like `require.resolve()`, on behalf of files. */
export declare const async: typeof resolve;

/** Synchronously resolve a module path, like `require.resolve()`, on behalf of files. */
export declare const sync: typeof resolve.sync;
