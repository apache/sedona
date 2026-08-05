"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                const { callee } = node;
                if (((0, ast_1.isTestIdentifier)(callee) || (0, ast_1.isDescribeCall)(node)) &&
                    callee.type === 'MemberExpression' &&
                    (0, ast_1.isPropertyAccessor)(callee, 'skip')) {
                    const isHook = (0, ast_1.isTest)(node) || (0, ast_1.isDescribeCall)(node);
                    context.report({
                        messageId: 'noSkippedTest',
                        node: isHook ? callee.property : node,
                        suggest: [
                            {
                                fix: (fixer) => {
                                    return isHook
                                        ? fixer.removeRange([
                                            callee.property.range[0] - 1,
                                            callee.range[1],
                                        ])
                                        : fixer.remove(node.parent);
                                },
                                messageId: 'removeSkippedTestAnnotation',
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
            description: 'Prevent usage of the `.skip()` skip test annotation.',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-skipped-test.md',
        },
        hasSuggestions: true,
        messages: {
            noSkippedTest: 'Unexpected use of the `.skip()` annotation.',
            removeSkippedTestAnnotation: 'Remove the `.skip()` annotation.',
        },
        type: 'suggestion',
    },
};
