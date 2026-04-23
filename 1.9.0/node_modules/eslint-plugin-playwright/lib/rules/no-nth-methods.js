"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const methods = new Set(['first', 'last', 'nth']);
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if (node.callee.type !== 'MemberExpression')
                    return;
                const method = (0, ast_1.getStringValue)(node.callee.property);
                if (!methods.has(method))
                    return;
                context.report({
                    data: { method },
                    loc: {
                        end: node.loc.end,
                        start: node.callee.property.loc.start,
                    },
                    messageId: 'noNthMethod',
                });
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Disallow usage of nth methods',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-nth-methods.md',
        },
        messages: {
            noNthMethod: 'Unexpected use of {{method}}()',
        },
        type: 'problem',
    },
};
