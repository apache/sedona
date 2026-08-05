"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const messageId = 'noNetworkidle';
const methods = new Set([
    'goBack',
    'goForward',
    'goto',
    'reload',
    'setContent',
    'waitForLoadState',
    'waitForURL',
]);
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if (node.callee.type !== 'MemberExpression')
                    return;
                const methodName = (0, ast_1.getStringValue)(node.callee.property);
                if (!methods.has(methodName))
                    return;
                // waitForLoadState has a single string argument
                if (methodName === 'waitForLoadState') {
                    const arg = node.arguments[0];
                    if (arg && (0, ast_1.isStringLiteral)(arg, 'networkidle')) {
                        context.report({ messageId, node: arg });
                    }
                    return;
                }
                // All other methods have an options object
                if (node.arguments.length >= 2) {
                    const [_, arg] = node.arguments;
                    if (arg.type !== 'ObjectExpression')
                        return;
                    const property = arg.properties
                        .filter((p) => p.type === 'Property')
                        .find((p) => (0, ast_1.isStringLiteral)(p.value, 'networkidle'));
                    if (property) {
                        context.report({ messageId, node: property.value });
                    }
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Possible Errors',
            description: 'Prevent usage of the networkidle option',
            recommended: true,
        },
        messages: {
            noNetworkidle: 'Unexpected use of networkidle.',
        },
        type: 'problem',
    },
};
