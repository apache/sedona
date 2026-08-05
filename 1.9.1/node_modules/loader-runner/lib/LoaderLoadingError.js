"use strict";

class LoadingLoaderError extends Error {
	constructor(message) {
		super(message);
		this.name = "LoaderRunnerError";

		if (Error.captureStackTrace) {
			// eslint-disable-next-line unicorn/no-useless-error-capture-stack-trace
			Error.captureStackTrace(this, this.constructor);
		}
	}
}

module.exports = LoadingLoaderError;
