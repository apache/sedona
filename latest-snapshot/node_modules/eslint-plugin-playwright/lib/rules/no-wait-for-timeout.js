"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if ((0, ast_1.isPageMethod)(node, 'waitForTimeout')) {
                    context.report({
                        messageId: 'noWaitForTimeout',
                        node,
                        suggest: [
                            {
                                fix: (fixer) => fixer.remove(node.parent && node.parent.type !== 'AwaitExpression'
                                    ? node.parent
                                    : node.parent.parent),
                                messageId: 'removeWaitForTimeout',
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
            description: 'Prevent usage of page.waitForTimeout()',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-wait-for-timeout.md',
        },
        hasSuggestions: true,
        messages: {
            noWaitForTimeout: 'Unexpected use of page.waitForTimeout().',
            removeWaitForTimeout: 'Remove the page.waitForTimeout() method.',
        },
        type: 'suggestion',
    },
};
