"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
function getPropertyRange(node) {
    return node.type === 'Identifier'
        ? node.range
        : [node.range[0] + 1, node.range[1] - 1];
}
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if ((0, ast_1.isPageMethod)(node, '$') || (0, ast_1.isPageMethod)(node, '$$')) {
                    context.report({
                        messageId: 'noElementHandle',
                        node: node.callee,
                        suggest: [
                            {
                                fix: (fixer) => {
                                    const { property } = node.callee;
                                    // Replace $/$$ with locator
                                    const fixes = [
                                        fixer.replaceTextRange(getPropertyRange(property), 'locator'),
                                    ];
                                    // Remove the await expression if it exists as locators do
                                    // not need to be awaited.
                                    if (node.parent.type === 'AwaitExpression') {
                                        fixes.push(fixer.removeRange([node.parent.range[0], node.range[0]]));
                                    }
                                    return fixes;
                                },
                                messageId: (0, ast_1.isPageMethod)(node, '$')
                                    ? 'replaceElementHandleWithLocator'
                                    : 'replaceElementHandlesWithLocator',
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
            description: 'The use of ElementHandle is discouraged, use Locator instead',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-element-handle.md',
        },
        hasSuggestions: true,
        messages: {
            noElementHandle: 'Unexpected use of element handles.',
            replaceElementHandlesWithLocator: 'Replace `page.$$` with `page.locator`',
            replaceElementHandleWithLocator: 'Replace `page.$` with `page.locator`',
        },
        type: 'suggestion',
    },
};
