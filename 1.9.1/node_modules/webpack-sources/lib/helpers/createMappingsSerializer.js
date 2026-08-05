/*
	MIT License http://www.opensource.org/licenses/mit-license.php
	Author Tobias Koppers @sokra
*/

"use strict";

/**
 * @callback MappingsSerializer
 * @param {number} generatedLine generated line
 * @param {number} generatedColumn generated column
 * @param {number} sourceIndex source index
 * @param {number} originalLine original line
 * @param {number} originalColumn generated line
 * @param {number} nameIndex generated line
 * @returns {string} result
 */

const ALPHABET = [
	..."ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
];

const CONTINUATION_BIT = 0x20;

/**
 * Append a VLQ-encoded signed integer to `str`. Hoisted to module scope so
 * that both serializers share a single function object and avoid allocating
 * a new closure on every call.
 * @param {string} str current string buffer
 * @param {number} value signed integer to encode
 * @returns {string} updated string buffer
 */
const writeValue = (str, value) => {
	const sign = (value >>> 31) & 1;
	const mask = value >> 31;
	const absValue = (value + mask) ^ mask;
	let data = (absValue << 1) | sign;
	for (;;) {
		const sextet = data & 0x1f;
		data >>= 5;
		if (data === 0) {
			return str + ALPHABET[sextet];
		}
		str += ALPHABET[sextet | CONTINUATION_BIT];
	}
};

const createFullMappingsSerializer = () => {
	let currentLine = 1;
	let currentColumn = 0;
	let currentSourceIndex = 0;
	let currentOriginalLine = 1;
	let currentOriginalColumn = 0;
	let currentNameIndex = 0;
	let activeMapping = false;
	let activeName = false;
	let initial = true;
	/** @type {MappingsSerializer} */
	return (
		generatedLine,
		generatedColumn,
		sourceIndex,
		originalLine,
		originalColumn,
		nameIndex,
	) => {
		if (activeMapping && currentLine === generatedLine) {
			// A mapping is still active
			if (
				sourceIndex === currentSourceIndex &&
				originalLine === currentOriginalLine &&
				originalColumn === currentOriginalColumn &&
				!activeName &&
				nameIndex < 0
			) {
				// avoid repeating the same original mapping
				return "";
			}
		}
		// No mapping is active
		else if (sourceIndex < 0) {
			// avoid writing unneccessary generated mappings
			return "";
		}

		let str;
		if (currentLine < generatedLine) {
			// Consecutive lines (diff === 1) are the dominant case; avoid the
			// `.repeat()` call entirely for them.
			str =
				generatedLine === currentLine + 1
					? ";"
					: ";".repeat(generatedLine - currentLine);
			currentLine = generatedLine;
			currentColumn = 0;
			initial = false;
		} else if (initial) {
			str = "";
			initial = false;
		} else {
			str = ",";
		}

		str = writeValue(str, generatedColumn - currentColumn);
		currentColumn = generatedColumn;
		if (sourceIndex >= 0) {
			activeMapping = true;
			if (sourceIndex === currentSourceIndex) {
				str += "A";
			} else {
				str = writeValue(str, sourceIndex - currentSourceIndex);
				currentSourceIndex = sourceIndex;
			}
			str = writeValue(str, originalLine - currentOriginalLine);
			currentOriginalLine = originalLine;
			if (originalColumn === currentOriginalColumn) {
				str += "A";
			} else {
				str = writeValue(str, originalColumn - currentOriginalColumn);
				currentOriginalColumn = originalColumn;
			}
			if (nameIndex >= 0) {
				str = writeValue(str, nameIndex - currentNameIndex);
				currentNameIndex = nameIndex;
				activeName = true;
			} else {
				activeName = false;
			}
		} else {
			activeMapping = false;
		}
		return str;
	};
};

const createLinesOnlyMappingsSerializer = () => {
	let lastWrittenLine = 0;
	let currentLine = 1;
	let currentSourceIndex = 0;
	let currentOriginalLine = 1;
	/** @type {MappingsSerializer} */
	return (
		generatedLine,
		_generatedColumn,
		sourceIndex,
		originalLine,
		_originalColumn,
		_nameIndex,
	) => {
		if (sourceIndex < 0) {
			// avoid writing generated mappings at all
			return "";
		}
		if (lastWrittenLine === generatedLine) {
			// avoid writing multiple original mappings per line
			return "";
		}
		let str;
		lastWrittenLine = generatedLine;
		if (generatedLine === currentLine + 1) {
			currentLine = generatedLine;
			if (sourceIndex === currentSourceIndex) {
				if (originalLine === currentOriginalLine + 1) {
					currentOriginalLine = originalLine;
					return ";AACA";
				}
				str = ";AA";
				str = writeValue(str, originalLine - currentOriginalLine);
				currentOriginalLine = originalLine;
				return `${str}A`;
			}
			str = ";A";
			str = writeValue(str, sourceIndex - currentSourceIndex);
			currentSourceIndex = sourceIndex;
			str = writeValue(str, originalLine - currentOriginalLine);
			currentOriginalLine = originalLine;
			return `${str}A`;
		}
		str = ";".repeat(generatedLine - currentLine);
		currentLine = generatedLine;
		if (sourceIndex === currentSourceIndex) {
			if (originalLine === currentOriginalLine + 1) {
				currentOriginalLine = originalLine;
				return `${str}AACA`;
			}
			str += "AA";
			str = writeValue(str, originalLine - currentOriginalLine);
			currentOriginalLine = originalLine;
			return `${str}A`;
		}
		str += "A";
		str = writeValue(str, sourceIndex - currentSourceIndex);
		currentSourceIndex = sourceIndex;
		str = writeValue(str, originalLine - currentOriginalLine);
		currentOriginalLine = originalLine;
		return `${str}A`;
	};
};

/**
 * @param {{ columns?: boolean }=} options options
 * @returns {MappingsSerializer} mappings serializer
 */
const createMappingsSerializer = (options) => {
	const linesOnly = options && options.columns === false;
	return linesOnly
		? createLinesOnlyMappingsSerializer()
		: createFullMappingsSerializer();
};

module.exports = createMappingsSerializer;
