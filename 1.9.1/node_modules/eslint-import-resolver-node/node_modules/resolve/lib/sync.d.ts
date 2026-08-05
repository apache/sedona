import type resolve = require('../index.js');

/**
 * Synchronously resolve a module path, like `require.resolve()`, on behalf of files.
 *
 * @param id - The module identifier to resolve.
 * @param options - Resolution options.
 * @returns The resolved file path.
 * @throws If the module cannot be found.
 */
declare function resolveSync(id: string, options?: resolve.SyncOptions): string;

export = resolveSync;
