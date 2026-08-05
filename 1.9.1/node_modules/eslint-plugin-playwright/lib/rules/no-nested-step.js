"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
function isStepCall(node) {
    const inner = node.type === 'CallExpression' ? node.callee : node;
    if (inner.type !== 'MemberExpression') {
        return false;
    }
    return (0, ast_1.isPropertyAccessor)(inner, 'step');
}
exports.default = {
    create(context) {
        const stack = [];
        function pushStepCallback(node) {
            if (node.parent.type !== 'CallExpression' || !isStepCall(node.parent)) {
                return;
            }
            stack.push(0);
            if (stack.length > 1) {
                context.report({
                    messageId: 'noNestedStep',
                    node: node.parent.callee,
                });
            }
        }
        function popStepCallback(node) {
            const { parent } = node;
            if (parent.type === 'CallExpression' && isStepCall(parent)) {
                stack.pop();
            }
        }
        return {
            ArrowFunctionExpression: pushStepCallback,
            'ArrowFunctionExpression:exit': popStepCallback,
            FunctionExpression: pushStepCallback,
            'FunctionExpression:exit': popStepCallback,
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Disallow nested `test.step()` methods',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-nested-step.md',
        },
        messages: {
            noNestedStep: 'Do not nest `test.step()` methods.',
        },
        schema: [],
        type: 'problem',
    },
};
