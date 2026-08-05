"use strict";

const LoaderLoadingError = require("./LoaderLoadingError");

let url;

function handleResult(loader, module, callback) {
	if (typeof module !== "function" && typeof module !== "object") {
		return callback(
			new LoaderLoadingError(
				`Module '${loader.path}' is not a loader (export function or es6 module)`
			)
		);
	}

	loader.normal = typeof module === "function" ? module : module.default;
	loader.pitch = module.pitch;
	loader.raw = module.raw;

	if (
		typeof loader.normal !== "function" &&
		typeof loader.pitch !== "function"
	) {
		return callback(
			new LoaderLoadingError(
				`Module '${loader.path}' is not a loader (must have normal or pitch function)`
			)
		);
	}
	callback();
}

function loadLoader(loader, callback) {
	if (loader.type === "module") {
		try {
			if (url === undefined) url = require("url");

			// eslint-disable-next-line n/no-unsupported-features/node-builtins
			const loaderUrl = url.pathToFileURL(loader.path);
			// Use `eval` so older parsers (and the main module resolver) don't
			// need to recognize the dynamic `import()` syntax at load time.
			// eslint-disable-next-line no-eval
			const modulePromise = eval(
				`import(${JSON.stringify(loaderUrl.toString())})`
			);

			modulePromise.then((module) => {
				handleResult(loader, module, callback);
			}, callback);
		} catch (err) {
			callback(err);
		}
		return;
	}

	let loadedModule;
	try {
		loadedModule = require(loader.path);
	} catch (err) {
		// It is possible for node to choke on a require if the FD descriptor
		// limit has been reached. Give it a chance to recover by deferring.
		if (err instanceof Error && err.code === "EMFILE") {
			return setImmediate(loadLoader, loader, callback);
		}
		return callback(err);
	}

	return handleResult(loader, loadedModule, callback);
}

module.exports = loadLoader;
