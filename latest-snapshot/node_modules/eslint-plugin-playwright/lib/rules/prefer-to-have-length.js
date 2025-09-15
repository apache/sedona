"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const fixer_1 = require("../utils/fixer");
const parseExpectCall_1 = require("../utils/parseExpectCall");
const lengthMatchers = new Set(['toBe', 'toEqual', 'toStrictEqual']);
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                const expectCall = (0, parseExpectCall_1.parseExpectCall)(node);
                if (!expectCall || !lengthMatchers.has(expectCall.matcherName)) {
                    return;
                }
                const [argument] = node.arguments;
                if (argument?.type !== 'MemberExpression' ||
                    !(0, ast_1.isPropertyAccessor)(argument, 'length')) {
                    return;
                }
                context.report({
                    fix(fixer) {
                        return [
                            // remove the "length" property accessor
                            fixer.removeRange([
                                argument.property.range[0] - 1,
                                argument.range[1],
                            ]),
                            // replace the current matcher with "toHaveLength"
                            (0, fixer_1.replaceAccessorFixer)(fixer, expectCall.matcher, 'toHaveLength'),
                        ];
                    },
                    messageId: 'useToHaveLength',
                    node: expectCall.matcher,
                });
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Suggest using `toHaveLength()`',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/prefer-to-have-length.md',
        },
        fixable: 'code',
        messages: {
            useToHaveLength: 'Use toHaveLength() instead',
        },
        schema: [],
        type: 'suggestion',
    },
};
