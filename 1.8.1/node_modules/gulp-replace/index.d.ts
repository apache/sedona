/// <reference types="node" />
import File = require("vinyl");

/**
 * Represents options for `gulp-replace`.
 */
interface Options {
    /**
     * A value indicating whether binary files should be skipped.
     */
    skipBinary?: boolean
}

/**
 * The context of the replacer-function.
 */
interface ReplacerContext {
    /**
     * The file being processed.
     */
    file: File
}

/**
 * Represents a method for replacing contents of a vinyl-file.
 */
type Replacer = (this: ReplacerContext, match: string, ...args: any[]) => string;

/**
 * Searches and replaces a portion of text using a `string` or a `RegExp`.
 *
 * @param search       The `string` or `RegExp` to search for.
 *
 * @param replacement  The replacement string or a function for generating a replacement.
 *
 *                     If `replacement` is a function, it will be called once for each match and will be passed the string
 *                     that is to be replaced. The value of `this.file` will be equal to the vinyl instance for the file
 *                     being processed.
 *
 *                     Read more at [`String.prototype.replace()` at MDN web docs](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/replace#Specifying_a_string_as_a_parameter").
 *
 * @param options      `options.skipBinary` will be equal to `true` by default.
 *
 *                     Skip binary files. This option is `true` by default. If
 *                     you want to replace content in binary files, you must explicitly set it to `false`.
 */
declare function replace(
    search: string | RegExp,
    replacement: string | Replacer,
    options?: Options
): NodeJS.ReadWriteStream;

export = replace;
