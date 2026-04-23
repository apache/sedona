"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if (((0, ast_1.isTest)(node) || (0, ast_1.isDescribeCall)(node)) &&
                    node.callee.type === 'MemberExpression' &&
                    (0, ast_1.isPropertyAccessor)(node.callee, 'only')) {
                    const { callee } = node;
                    context.report({
                        messageId: 'noFocusedTest',
                        node: node.callee.property,
                        suggest: [
                            {
                                // - 1 to remove the `.only` annotation with dot notation
                                fix: (fixer) => fixer.removeRange([
                                    callee.property.range[0] - 1,
                                    callee.range[1],
                                ]),
                                messageId: 'suggestRemoveOnly',
                            },
                        ],
                    });
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Possible Errors',
            description: 'Prevent usage of `.only()` focus test annotation',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-focused-test.md',
        },
        hasSuggestions: true,
        messages: {
            noFocusedTest: 'Unexpected focused test.',
            suggestRemoveOnly: 'Remove .only() annotation.',
        },
        type: 'problem',
    },
};
