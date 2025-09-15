"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const misc_1 = require("../utils/misc");
const parseExpectCall_1 = require("../utils/parseExpectCall");
function isMatcherCalled(node) {
    if (node.parent.type !== 'MemberExpression') {
        // Just asserting that the parent is a call expression is not enough as
        // the node could be an argument of a call expression which doesn't
        // determine if it is called. To determine if it is called, we verify
        // that the parent call expression callee is the same as the node.
        return {
            called: node.parent.type === 'CallExpression' && node.parent.callee === node,
            node,
        };
    }
    // If the parent is a member expression, we continue traversing upward to
    // handle matcher chains of unknown length. e.g. expect().not.something.
    return isMatcherCalled(node.parent);
}
exports.default = {
    create(context) {
        const options = {
            maxArgs: 2,
            minArgs: 1,
            ...(context.options?.[0] ?? {}),
        };
        const minArgs = Math.min(options.minArgs, options.maxArgs);
        const maxArgs = Math.max(options.minArgs, options.maxArgs);
        return {
            CallExpression(node) {
                if (!(0, ast_1.isExpectCall)(node))
                    return;
                const expectCall = (0, parseExpectCall_1.parseExpectCall)(node);
                if (!expectCall) {
                    context.report({ messageId: 'matcherNotFound', node });
                }
                else {
                    const result = isMatcherCalled(node);
                    if (!result.called) {
                        context.report({
                            messageId: 'matcherNotCalled',
                            node: result.node.type === 'MemberExpression'
                                ? result.node.property
                                : result.node,
                        });
                    }
                }
                if (node.arguments.length < minArgs) {
                    context.report({
                        data: (0, misc_1.getAmountData)(minArgs),
                        messageId: 'notEnoughArgs',
                        node,
                    });
                }
                if (node.arguments.length > maxArgs) {
                    context.report({
                        data: (0, misc_1.getAmountData)(maxArgs),
                        messageId: 'tooManyArgs',
                        node,
                    });
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Possible Errors',
            description: 'Enforce valid `expect()` usage',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/valid-expect.md',
        },
        messages: {
            matcherNotCalled: 'Matchers must be called to assert.',
            matcherNotFound: 'Expect must have a corresponding matcher call.',
            notEnoughArgs: 'Expect requires at least {{amount}} argument{{s}}.',
            tooManyArgs: 'Expect takes at most {{amount}} argument{{s}}.',
        },
        schema: [
            {
                additionalProperties: false,
                properties: {
                    maxArgs: {
                        minimum: 1,
                        type: 'number',
                    },
                    minArgs: {
                        minimum: 1,
                        type: 'number',
                    },
                },
                type: 'object',
            },
        ],
        type: 'problem',
    },
};
