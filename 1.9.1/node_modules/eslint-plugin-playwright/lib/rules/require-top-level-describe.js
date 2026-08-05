"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const misc_1 = require("../utils/misc");
exports.default = {
    create(context) {
        const { maxTopLevelDescribes } = {
            maxTopLevelDescribes: Infinity,
            ...(context.options?.[0] ?? {}),
        };
        let topLevelDescribeCount = 0;
        let describeCount = 0;
        return {
            CallExpression(node) {
                if ((0, ast_1.isDescribeCall)(node)) {
                    describeCount++;
                    if (describeCount === 1) {
                        topLevelDescribeCount++;
                        if (topLevelDescribeCount > maxTopLevelDescribes) {
                            context.report({
                                data: (0, misc_1.getAmountData)(maxTopLevelDescribes),
                                messageId: 'tooManyDescribes',
                                node: node.callee,
                            });
                        }
                    }
                }
                else if (!describeCount) {
                    if ((0, ast_1.isTest)(node)) {
                        context.report({ messageId: 'unexpectedTest', node: node.callee });
                    }
                    else if ((0, ast_1.isTestHook)(node)) {
                        context.report({ messageId: 'unexpectedHook', node: node.callee });
                    }
                }
            },
            'CallExpression:exit'(node) {
                if ((0, ast_1.isDescribeCall)(node)) {
                    describeCount--;
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Require test cases and hooks to be inside a `test.describe` block',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/require-top-level-describe.md',
        },
        messages: {
            tooManyDescribes: 'There should not be more than {{max}} describe{{s}} at the top level',
            unexpectedHook: 'All hooks must be wrapped in a describe block.',
            unexpectedTest: 'All test cases must be wrapped in a describe block.',
        },
        schema: [
            {
                additionalProperties: false,
                properties: {
                    maxTopLevelDescribes: {
                        minimum: 1,
                        type: 'number',
                    },
                },
                type: 'object',
            },
        ],
        type: 'suggestion',
    },
};
