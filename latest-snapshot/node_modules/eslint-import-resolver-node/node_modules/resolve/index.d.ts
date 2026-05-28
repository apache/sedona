/**
 * Asynchronously resolve a module path, like `require.resolve()`, on behalf of files.
 *
 * @param id - The module identifier to resolve.
 * @param options - Resolution options.
 * @param callback - Called with `(err, resolved, pkg)` when resolution completes.
 */
declare function resolve(id: string, callback: resolve.Callback): void;
declare function resolve(id: string, options: resolve.AsyncOptions, callback: resolve.Callback): void;

declare namespace resolve {
  /** Asynchronously resolve a module path, like `require.resolve()`, on behalf of files. */
  function async(id: string, callback: Callback): void;
  function async(id: string, options: AsyncOptions, callback: Callback): void;

  /**
   * Synchronously resolve a module path, like `require.resolve()`, on behalf of files.
   *
   * @param id - The module identifier to resolve.
   * @param options - Resolution options.
   * @returns The resolved file path.
   * @throws If the module cannot be found.
   */
  function sync(id: string, options?: SyncOptions): string;

  /** A parsed package.json object. */
  interface PackageJSON {
    [key: string]: unknown;
    name?: string;
    main?: string;
    exports?: unknown;
    engines?: { node?: string };
  }

  /** Callback for asynchronous resolution. */
  type Callback = (err: Error | null, resolved?: string, pkg?: PackageJSON) => void;

  /** Options shared by both async and sync resolution. */
  interface BaseOptions {
    /** Directory to resolve from. Defaults to `__dirname` of the calling file. */
    basedir?: string;
    /** The `package.json` object associated with the calling module. */
    package?: PackageJSON;
    /** File extensions to search, in order. Defaults to `['.js']`. */
    extensions?: ReadonlyArray<string>;
    /** Whether to include core modules (e.g. `fs`, `path`) in results. Defaults to `true`. */
    includeCoreModules?: boolean;
    /** Whether to preserve symlinks instead of resolving them. Defaults to `false`. */
    preserveSymlinks?: boolean;
    /** Additional lookup paths. */
    paths?: ReadonlyArray<string> | ((request: string, start: string, getNodeModulesDirs: () => string[], opts: BaseOptions) => string[]);
    /** The filename used for error messages and as a fallback for basedir. */
    filename?: string;
    /** The directory name(s) to use for node_modules lookups. Defaults to `['node_modules']`. */
    moduleDirectory?: string | ReadonlyArray<string>;
    /**
     * Transform a package.json object before its `main` field is used.
     *
     * @param pkg - The parsed package.json.
     * @param pkgFile - The path to the package.json file.
     * @param dir - The directory containing the package.json.
     * @returns The (possibly modified) package.json object.
     */
    packageFilter?: (pkg: PackageJSON, pkgFile: string, dir: string) => PackageJSON;
    /**
     * Transform a resolved path before it is used.
     *
     * @param pkg - The parsed package.json.
     * @param path - The resolved path.
     * @param relativePath - The path relative to the package directory.
     * @returns An alternative path, or undefined/falsy to use the original.
     */
    pathFilter?: (pkg: PackageJSON, path: string, relativePath: string) => string | undefined;
    /**
     * Override the default node_modules candidate iterator.
     *
     * @param request - The module being resolved.
     * @param start - The starting directory.
     * @param thunk - A function returning the default candidate directories.
     * @param opts - The resolve options.
     * @returns An array of candidate directories.
     */
    packageIterator?: (request: string, start: string, thunk: () => string[], opts: BaseOptions) => string[];
    /**
     * An exports category string, as defined by `node-exports-info`.
     * Mutually exclusive with `engines`.
     */
    exportsCategory?: string;
    /**
     * When `true`, reads the consumer's `engines.node` to determine the exports category.
     * When a string, it is treated as a semver range for engines.node.
     * Mutually exclusive with `exportsCategory`.
     */
    engines?: boolean | string;
    /** Custom conditions for package.json `exports` resolution. */
    conditions?: ReadonlyArray<string>;
  }

  /** Options for asynchronous resolution. */
  interface AsyncOptions extends BaseOptions {
    /**
     * Check whether a path is a file.
     *
     * @param file - The path to check.
     * @param cb - Called with `(err, isFile)`.
     */
    isFile?: (file: string, cb: (err: Error | null, isFile?: boolean) => void) => void;
    /**
     * Check whether a path is a directory.
     *
     * @param dir - The path to check.
     * @param cb - Called with `(err, isDirectory)`.
     */
    isDirectory?: (dir: string, cb: (err: Error | null, isDirectory?: boolean) => void) => void;
    /**
     * Resolve a path's real location, following symlinks.
     *
     * @param file - The path to resolve.
     * @param cb - Called with `(err, realPath)`.
     */
    realpath?: (file: string, cb: (err: Error | null, realPath?: string) => void) => void;
    /**
     * Read a file's contents.
     * Mutually exclusive with `readPackage`.
     *
     * @param file - The path to read.
     * @param cb - Called with `(err, contents)`.
     */
    readFile?: (file: string, cb: (err: Error | null, contents?: string | Buffer) => void) => void;
    /**
     * Read and parse a package.json file.
     * Mutually exclusive with `readFile`.
     *
     * @param readFile - The file-reading function.
     * @param pkgFile - The path to the package.json.
     * @param cb - Called with `(err, pkg)`.
     */
    readPackage?: (readFile: (file: string, cb: (err: Error | null, contents?: string | Buffer) => void) => void, pkgFile: string, cb: (err: Error | null, pkg?: PackageJSON) => void) => void;
  }

  /** Options for synchronous resolution. */
  interface SyncOptions extends BaseOptions {
    /**
     * Check whether a path is a file.
     *
     * @param file - The path to check.
     * @returns `true` if the path is a file.
     */
    isFile?: (file: string) => boolean;
    /**
     * Check whether a path is a directory.
     *
     * @param dir - The path to check.
     * @returns `true` if the path is a directory.
     */
    isDirectory?: (dir: string) => boolean;
    /**
     * Resolve a path's real location, following symlinks.
     *
     * @param file - The path to resolve.
     * @returns The resolved real path.
     */
    realpathSync?: (file: string) => string;
    /**
     * Read a file's contents synchronously.
     * Mutually exclusive with `readPackageSync`.
     *
     * @param file - The path to read.
     * @returns The file contents.
     */
    readFileSync?: (file: string) => string | Buffer;
    /**
     * Read and parse a package.json file synchronously.
     * Mutually exclusive with `readFileSync`.
     *
     * @param readFileSync - The synchronous file-reading function.
     * @param pkgFile - The path to the package.json.
     * @returns The parsed package.json object.
     */
    readPackageSync?: (readFileSync: (file: string) => string | Buffer, pkgFile: string) => PackageJSON;
  }
}

export = resolve;
