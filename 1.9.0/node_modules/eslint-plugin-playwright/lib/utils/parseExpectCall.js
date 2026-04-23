"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseExpectCall = void 0;
const ast_1 = require("./ast");
const MODIFIER_NAMES = new Set(['not', 'resolves', 'rejects']);
function getExpectArguments(node) {
    const grandparent = node.parent.parent;
    return grandparent.type === 'CallExpression' ? grandparent.arguments : [];
}
function parseExpectCall(node) {
    if (!(0, ast_1.isExpectCall)(node)) {
        return;
    }
    const members = (0, ast_1.getMatchers)(node);
    const modifiers = [];
    let matcher;
    // Separate the matchers (e.g. toBe) from modifiers (e.g. not)
    members.forEach((item) => {
        if (MODIFIER_NAMES.has((0, ast_1.getStringValue)(item))) {
            modifiers.push(item);
        }
        else {
            matcher = item;
        }
    });
    // Rules only run against full expect calls with matchers
    if (!matcher) {
        return;
    }
    return {
        args: getExpectArguments(matcher),
        matcher,
        matcherName: (0, ast_1.getStringValue)(matcher),
        members,
        modifiers,
    };
}
exports.parseExpectCall = parseExpectCall;
