"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if ((0, ast_1.getExpectType)(node) === 'standalone') {
                    context.report({
                        fix: (fixer) => fixer.insertTextAfter(node.callee, '.soft'),
                        messageId: 'requireSoft',
                        node: node.callee,
                    });
                }
            },
        };
    },
    meta: {
        docs: {
            description: 'Require all assertions to use `expect.soft`',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/require-soft-assertions.md',
        },
        fixable: 'code',
        messages: {
            requireSoft: 'Unexpected non-soft assertion',
        },
        schema: [],
        type: 'suggestion',
    },
};
