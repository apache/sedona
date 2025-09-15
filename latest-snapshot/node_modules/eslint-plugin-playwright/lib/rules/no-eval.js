"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                const isEval = (0, ast_1.isPageMethod)(node, '$eval');
                if (isEval || (0, ast_1.isPageMethod)(node, '$$eval')) {
                    context.report({
                        messageId: isEval ? 'noEval' : 'noEvalAll',
                        node: node.callee,
                    });
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Possible Errors',
            description: 'The use of `page.$eval` and `page.$$eval` are discouraged, use `locator.evaluate` or `locator.evaluateAll` instead',
            recommended: true,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-eval.md',
        },
        messages: {
            noEval: 'Unexpected use of page.$eval().',
            noEvalAll: 'Unexpected use of page.$$eval().',
        },
        type: 'problem',
    },
};
