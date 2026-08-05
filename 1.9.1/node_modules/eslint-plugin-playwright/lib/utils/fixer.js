"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.replaceAccessorFixer = exports.getRangeOffset = void 0;
const getRangeOffset = (node) => node.type === 'Identifier' ? 0 : 1;
exports.getRangeOffset = getRangeOffset;
/**
 * Replaces an accessor node with the given `text`.
 *
 * This ensures that fixes produce valid code when replacing both dot-based and
 * bracket-based property accessors.
 */
function replaceAccessorFixer(fixer, node, text) {
    const [start, end] = node.range;
    return fixer.replaceTextRange([start + (0, exports.getRangeOffset)(node), end - (0, exports.getRangeOffset)(node)], text);
}
exports.replaceAccessorFixer = replaceAccessorFixer;
