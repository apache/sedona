"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        function checkConditional(node) {
            const call = (0, ast_1.findParent)(node, 'CallExpression');
            if (call && (0, ast_1.isTest)(call)) {
                context.report({ messageId: 'conditionalInTest', node });
            }
        }
        return {
            ConditionalExpression: checkConditional,
            IfStatement: checkConditional,
            LogicalExpression: checkConditional,
            SwitchStatement: checkConditional,
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Disallow conditional logic in tests',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-conditional-in-test.md',
        },
        messages: {
            conditionalInTest: 'Avoid having conditionals in tests',
        },
        schema: [],
        type: 'problem',
    },
};
