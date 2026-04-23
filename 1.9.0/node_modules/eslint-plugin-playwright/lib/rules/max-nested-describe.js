"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        const { options } = context;
        const max = options[0]?.max ?? 5;
        const describeCallbackStack = [];
        function pushDescribeCallback(node) {
            if (node.parent.type !== 'CallExpression' ||
                !(0, ast_1.isDescribeCall)(node.parent)) {
                return;
            }
            describeCallbackStack.push(0);
            if (describeCallbackStack.length > max) {
                context.report({
                    data: {
                        depth: describeCallbackStack.length.toString(),
                        max: max.toString(),
                    },
                    messageId: 'exceededMaxDepth',
                    node: node.parent.callee,
                });
            }
        }
        function popDescribeCallback(node) {
            const { parent } = node;
            if (parent.type === 'CallExpression' && (0, ast_1.isDescribeCall)(parent)) {
                describeCallbackStack.pop();
            }
        }
        return {
            ArrowFunctionExpression: pushDescribeCallback,
            'ArrowFunctionExpression:exit': popDescribeCallback,
            FunctionExpression: pushDescribeCallback,
            'FunctionExpression:exit': popDescribeCallback,
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Enforces a maximum depth to nested describe calls',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/max-nested-describe.md',
        },
        messages: {
            exceededMaxDepth: 'Maximum describe call depth exceeded ({{ depth }}). Maximum allowed is {{ max }}.',
        },
        schema: [
            {
                additionalProperties: false,
                properties: {
                    max: {
                        minimum: 0,
                        type: 'integer',
                    },
                },
                type: 'object',
            },
        ],
        type: 'suggestion',
    },
};
