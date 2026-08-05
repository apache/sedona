"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fixer_1 = require("../utils/fixer");
const parseExpectCall_1 = require("../utils/parseExpectCall");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                const expectCall = (0, parseExpectCall_1.parseExpectCall)(node);
                if (expectCall?.matcherName === 'toEqual') {
                    context.report({
                        messageId: 'useToStrictEqual',
                        node: expectCall.matcher,
                        suggest: [
                            {
                                fix: (fixer) => {
                                    return (0, fixer_1.replaceAccessorFixer)(fixer, expectCall.matcher, 'toStrictEqual');
                                },
                                messageId: 'suggestReplaceWithStrictEqual',
                            },
                        ],
                    });
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Suggest using `toStrictEqual()`',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/prefer-strict-equal.md',
        },
        fixable: 'code',
        hasSuggestions: true,
        messages: {
            suggestReplaceWithStrictEqual: 'Replace with `toStrictEqual()`',
            useToStrictEqual: 'Use toStrictEqual() instead',
        },
        schema: [],
        type: 'suggestion',
    },
};
