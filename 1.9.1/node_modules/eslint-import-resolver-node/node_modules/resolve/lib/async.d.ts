import type resolve = require('../index.js');

/**
 * Asynchronously resolve a module path, like `require.resolve()`, on behalf of files.
 *
 * @param id - The module identifier to resolve.
 * @param options - Resolution options.
 * @param callback - Called with `(err, resolved, pkg)` when resolution completes.
 */
declare function resolveAsync(id: string, callback: resolve.Callback): void;
declare function resolveAsync(id: string, options: resolve.AsyncOptions, callback: resolve.Callback): void;

export = resolveAsync;
