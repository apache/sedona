/*
	MIT License http://www.opensource.org/licenses/mit-license.php
	Author Tobias Koppers @sokra
*/

"use strict";

const { readFile } = require("fs");

const loadLoader = require("./loadLoader");

const HASH_ESCAPE_REGEXP = /#/g;

// UTF-8 encoding of the BOM: EF BB BF
const UTF8_BOM_0 = 0xef;
const UTF8_BOM_1 = 0xbb;
const UTF8_BOM_2 = 0xbf;

function utf8BufferToString(buf) {
	// Detect and skip the BOM at the buffer level to avoid materializing the
	// prefix as JS string and then re-slicing it.
	if (
		buf.length >= 3 &&
		buf[0] === UTF8_BOM_0 &&
		buf[1] === UTF8_BOM_1 &&
		buf[2] === UTF8_BOM_2
	) {
		return buf.toString("utf8", 3);
	}
	return buf.toString("utf8");
}

/**
 * Escape `#` characters with a preceding `\0` byte. Short-circuits when the input contains no `#`, avoiding the regex scan for the common case.
 * @param {string} str input string
 * @returns {string} escaped string
 */
function escapeHash(str) {
	return str.includes("#") ? str.replace(HASH_ESCAPE_REGEXP, "\0#") : str;
}

const PATH_QUERY_FRAGMENT_REGEXP =
	/^((?:\0.|[^?#\0])*)(\?(?:\0.|[^#\0])*)?(#.*)?$/;
const ZERO_ESCAPE_REGEXP = /\0(.)/g;

/**
 * @param {string} identifier identifier
 * @returns {[string, string, string]} parsed identifier
 */
function parseIdentifier(identifier) {
	// Fast path for inputs that don't use \0 escaping.
	const firstEscape = identifier.indexOf("\0");

	if (firstEscape < 0) {
		const queryStart = identifier.indexOf("?");
		const fragmentStart = identifier.indexOf("#");

		if (fragmentStart < 0) {
			if (queryStart < 0) {
				// No fragment, no query
				return [identifier, "", ""];
			}

			// Query, no fragment
			return [
				identifier.slice(0, queryStart),
				identifier.slice(queryStart),
				"",
			];
		}

		if (queryStart < 0 || fragmentStart < queryStart) {
			// Fragment, no query
			return [
				identifier.slice(0, fragmentStart),
				"",
				identifier.slice(fragmentStart),
			];
		}

		// Query and fragment
		return [
			identifier.slice(0, queryStart),
			identifier.slice(queryStart, fragmentStart),
			identifier.slice(fragmentStart),
		];
	}

	const match = PATH_QUERY_FRAGMENT_REGEXP.exec(identifier);

	return [
		match[1].replace(ZERO_ESCAPE_REGEXP, "$1"),
		match[2] ? match[2].replace(ZERO_ESCAPE_REGEXP, "$1") : "",
		match[3] || "",
	];
}

function dirname(path) {
	if (path === "/") return "/";
	const i = path.lastIndexOf("/");
	const j = path.lastIndexOf("\\");
	const i2 = path.indexOf("/");
	const j2 = path.indexOf("\\");
	const idx = i > j ? i : j;
	const idx2 = i > j ? i2 : j2;
	if (idx < 0) return path;
	if (idx === idx2) return path.slice(0, idx + 1);
	return path.slice(0, idx);
}

function createLoaderObject(loader) {
	const obj = {
		path: null,
		query: null,
		fragment: null,
		options: null,
		ident: null,
		normal: null,
		pitch: null,
		raw: null,
		data: null,
		pitchExecuted: false,
		normalExecuted: false,
	};
	Object.defineProperty(obj, "request", {
		enumerable: true,
		get() {
			return escapeHash(obj.path) + escapeHash(obj.query) + obj.fragment;
		},
		set(value) {
			if (typeof value === "string") {
				const [path, query, fragment] = parseIdentifier(value);
				obj.path = path;
				obj.query = query;
				obj.fragment = fragment;
				obj.options = undefined;
				obj.ident = undefined;
				return;
			}

			if (!value.loader) {
				throw new Error(
					`request should be a string or object with loader and options (${JSON.stringify(
						value
					)})`
				);
			}

			const { loader: path, fragment, type, options, ident } = value;
			obj.path = path;
			obj.fragment = fragment || "";
			obj.type = type;
			obj.options = options;
			obj.ident = ident;

			if (options === null || options === undefined) {
				obj.query = "";
			} else if (typeof options === "string") {
				obj.query = `?${options}`;
			} else if (ident) {
				obj.query = `??${ident}`;
			} else if (typeof options === "object" && options.ident) {
				obj.query = `??${options.ident}`;
			} else {
				obj.query = `?${JSON.stringify(options)}`;
			}
		},
	});
	obj.request = loader;
	if (Object.preventExtensions) {
		Object.preventExtensions(obj);
	}
	return obj;
}

function runSyncOrAsync(fn, context, args, callback) {
	let isSync = true;
	let isDone = false;
	let isError = false; // internal error
	let reportedError = false;

	// eslint-disable-next-line func-name-matching
	const innerCallback = (context.callback = function innerCallback(
		...callbackArgs
	) {
		if (isDone) {
			if (reportedError) return; // ignore
			throw new Error("callback(): The callback was already called.");
		}

		isDone = true;
		isSync = false;

		try {
			callback(...callbackArgs);
		} catch (err) {
			isError = true;
			throw err;
		}
	});

	context.async = function async() {
		if (isDone) {
			if (reportedError) return; // ignore
			throw new Error("async(): The callback was already called.");
		}

		isSync = false;

		return innerCallback;
	};

	try {
		const result = (function LOADER_EXECUTION() {
			return fn.apply(context, args);
		})();
		if (isSync) {
			isDone = true;
			if (result === undefined) return callback();
			if (
				result &&
				typeof result === "object" &&
				typeof result.then === "function"
			) {
				return result.then((r) => {
					callback(null, r);
				}, callback);
			}
			return callback(null, result);
		}
	} catch (err) {
		if (isError) throw err;
		if (isDone) {
			// loader is already "done", so we cannot use the callback function
			// for better debugging we print the error on the console
			if (typeof err === "object" && err.stack) {
				// eslint-disable-next-line no-console
				console.error(err.stack);
			} else {
				// eslint-disable-next-line no-console
				console.error(err);
			}
			return;
		}
		isDone = true;
		reportedError = true;
		callback(err);
	}
}

function convertArgs(args, raw) {
	if (!raw && Buffer.isBuffer(args[0])) {
		args[0] = utf8BufferToString(args[0]);
	} else if (raw && typeof args[0] === "string") {
		args[0] = Buffer.from(args[0], "utf8");
	}
}

function iterateNormalLoaders(options, loaderContext, args, callback) {
	while (loaderContext.loaderIndex >= 0) {
		const currentLoaderObject =
			loaderContext.loaders[loaderContext.loaderIndex];

		if (currentLoaderObject.normalExecuted) {
			loaderContext.loaderIndex--;
			continue;
		}

		const fn = currentLoaderObject.normal;
		currentLoaderObject.normalExecuted = true;

		if (!fn) continue;

		convertArgs(args, currentLoaderObject.raw);

		return runSyncOrAsync(fn, loaderContext, args, (err, ...nextArgs) => {
			if (err) return callback(err);
			iterateNormalLoaders(options, loaderContext, nextArgs, callback);
		});
	}

	return callback(null, args);
}

function processResource(options, loaderContext, callback) {
	// set loader index to last loader
	loaderContext.loaderIndex = loaderContext.loaders.length - 1;

	const { resourcePath } = loaderContext;

	if (!resourcePath) {
		return iterateNormalLoaders(options, loaderContext, [null], callback);
	}

	options.processResource(loaderContext, resourcePath, (err, ...args) => {
		if (err) return callback(err);

		// eslint-disable-next-line prefer-destructuring
		options.resourceBuffer = args[0];

		iterateNormalLoaders(options, loaderContext, args, callback);
	});
}

function iteratePitchingLoaders(options, loaderContext, callback) {
	// Iterative walk over already-pitched loaders without recursion.
	while (loaderContext.loaderIndex < loaderContext.loaders.length) {
		const currentLoaderObject =
			loaderContext.loaders[loaderContext.loaderIndex];

		if (currentLoaderObject.pitchExecuted) {
			loaderContext.loaderIndex++;
			continue;
		}

		return loadLoader(currentLoaderObject, (err) => {
			if (err) {
				loaderContext.cacheable(false);
				return callback(err);
			}
			const fn = currentLoaderObject.pitch;
			currentLoaderObject.pitchExecuted = true;
			if (!fn) return iteratePitchingLoaders(options, loaderContext, callback);

			runSyncOrAsync(
				fn,
				loaderContext,
				[
					loaderContext.remainingRequest,
					loaderContext.previousRequest,
					(currentLoaderObject.data = {}),
				],
				(pitchErr, ...args) => {
					if (pitchErr) return callback(pitchErr);
					// Determine whether to continue the pitching process based on
					// argument values (as opposed to argument presence) in order
					// to support synchronous and asynchronous usages. Inline loop
					// avoids allocating a predicate closure per pitched loader.
					let hasArg = false;
					for (let i = 0; i < args.length; i++) {
						if (args[i] !== undefined) {
							hasArg = true;
							break;
						}
					}
					if (hasArg) {
						loaderContext.loaderIndex--;
						iterateNormalLoaders(options, loaderContext, args, callback);
					} else {
						iteratePitchingLoaders(options, loaderContext, callback);
					}
				}
			);
		});
	}

	// Reached the end: move on to processing the resource itself.
	return processResource(options, loaderContext, callback);
}

/**
 * Join loader requests into a single `!`-separated string for a range of loader indices.
 * @param {object[]} loaders loader objects
 * @param {number} start inclusive start index
 * @param {number} end exclusive end index
 * @param {string} resource resource string
 * @returns {string} joined request
 */
function joinRequests(loaders, start, end, resource) {
	let result = "";
	for (let i = start; i < end; i++) {
		result += `${loaders[i].request}!`;
	}
	return result + resource;
}

module.exports.getContext = function getContext(resource) {
	const [path] = parseIdentifier(resource);
	return dirname(path);
};

module.exports.runLoaders = function runLoaders(options, callback) {
	// read options
	const resource = options.resource || "";
	const loaderContext = options.context || {};
	const processResourceFn =
		options.processResource ||
		((readResource, context, res, cb) => {
			context.addDependency(res);
			readResource(res, cb);
		}).bind(null, options.readResource || readFile);

	const splittedResource = resource && parseIdentifier(resource);
	const resourcePath = splittedResource ? splittedResource[0] : "";
	const resourceQuery = splittedResource ? splittedResource[1] : "";
	const resourceFragment = splittedResource ? splittedResource[2] : "";
	const contextDirectory = resourcePath ? dirname(resourcePath) : null;

	// execution state
	let requestCacheable = true;
	const fileDependencies = [];
	const contextDependencies = [];
	const missingDependencies = [];

	// prepare loader objects
	const loaders = (options.loaders || []).map(createLoaderObject);

	loaderContext.context = contextDirectory;
	loaderContext.loaderIndex = 0;
	loaderContext.loaders = loaders;
	loaderContext.resourcePath = resourcePath;
	loaderContext.resourceQuery = resourceQuery;
	loaderContext.resourceFragment = resourceFragment;
	loaderContext.async = null;
	loaderContext.callback = null;
	loaderContext.cacheable = (flag) => {
		if (flag === false) {
			requestCacheable = false;
		}
	};
	loaderContext.dependency = loaderContext.addDependency = (file) => {
		fileDependencies.push(file);
	};
	loaderContext.addContextDependency = (context) => {
		contextDependencies.push(context);
	};
	loaderContext.addMissingDependency = (context) => {
		missingDependencies.push(context);
	};
	loaderContext.getDependencies = () => fileDependencies.slice();
	loaderContext.getContextDependencies = () => contextDependencies.slice();
	loaderContext.getMissingDependencies = () => missingDependencies.slice();
	loaderContext.clearDependencies = () => {
		fileDependencies.length = 0;
		contextDependencies.length = 0;
		missingDependencies.length = 0;
		requestCacheable = true;
	};
	Object.defineProperty(loaderContext, "resource", {
		enumerable: true,
		get() {
			return (
				escapeHash(loaderContext.resourcePath) +
				escapeHash(loaderContext.resourceQuery) +
				loaderContext.resourceFragment
			);
		},
		set(value) {
			const splitted = value && parseIdentifier(value);
			loaderContext.resourcePath = splitted ? splitted[0] : "";
			loaderContext.resourceQuery = splitted ? splitted[1] : "";
			loaderContext.resourceFragment = splitted ? splitted[2] : "";
		},
	});
	Object.defineProperty(loaderContext, "request", {
		enumerable: true,
		get() {
			return joinRequests(
				loaders,
				0,
				loaders.length,
				loaderContext.resource || ""
			);
		},
	});
	Object.defineProperty(loaderContext, "remainingRequest", {
		enumerable: true,
		get() {
			return joinRequests(
				loaders,
				loaderContext.loaderIndex + 1,
				loaders.length,
				loaderContext.resource
			);
		},
	});
	Object.defineProperty(loaderContext, "currentRequest", {
		enumerable: true,
		get() {
			return joinRequests(
				loaders,
				loaderContext.loaderIndex,
				loaders.length,
				loaderContext.resource
			);
		},
	});
	Object.defineProperty(loaderContext, "previousRequest", {
		enumerable: true,
		get() {
			const end = loaderContext.loaderIndex;
			if (end === 0) return "";
			let result = loaders[0].request;
			for (let i = 1; i < end; i++) {
				result += `!${loaders[i].request}`;
			}
			return result;
		},
	});
	Object.defineProperty(loaderContext, "query", {
		enumerable: true,
		get() {
			const entry = loaders[loaderContext.loaderIndex];
			return entry.options && typeof entry.options === "object"
				? entry.options
				: entry.query;
		},
	});
	Object.defineProperty(loaderContext, "data", {
		enumerable: true,
		get() {
			return loaders[loaderContext.loaderIndex].data;
		},
	});

	// finish loader context
	if (Object.preventExtensions) {
		Object.preventExtensions(loaderContext);
	}

	const processOptions = {
		resourceBuffer: null,
		processResource: processResourceFn,
	};
	iteratePitchingLoaders(processOptions, loaderContext, (err, result) => {
		if (err) {
			return callback(err, {
				cacheable: requestCacheable,
				fileDependencies,
				contextDependencies,
				missingDependencies,
			});
		}
		callback(null, {
			result,
			resourceBuffer: processOptions.resourceBuffer,
			cacheable: requestCacheable,
			fileDependencies,
			contextDependencies,
			missingDependencies,
		});
	});
};
