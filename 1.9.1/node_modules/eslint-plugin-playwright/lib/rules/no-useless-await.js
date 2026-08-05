"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const locatorMethods = new Set([
    'and',
    'first',
    'getByAltText',
    'getByLabel',
    'getByPlaceholder',
    'getByRole',
    'getByTestId',
    'getByText',
    'getByTitle',
    'last',
    'locator',
    'nth',
    'or',
]);
const pageMethods = new Set([
    'childFrames',
    'frame',
    'frameLocator',
    'frames',
    'isClosed',
    'isDetached',
    'mainFrame',
    'name',
    'on',
    'page',
    'parentFrame',
    'setDefaultNavigationTimeout',
    'setDefaultTimeout',
    'url',
    'video',
    'viewportSize',
    'workers',
]);
function isSupportedMethod(node) {
    if (node.callee.type !== 'MemberExpression')
        return false;
    const name = (0, ast_1.getStringValue)(node.callee.property);
    return (locatorMethods.has(name) ||
        (pageMethods.has(name) && (0, ast_1.isPageMethod)(node, name)));
}
exports.default = {
    create(context) {
        return {
            AwaitExpression(node) {
                // Must be a call expression
                if (node.argument.type !== 'CallExpression')
                    return;
                // Must be a foo.bar() call, bare calls are ignored
                const { callee } = node.argument;
                if (callee.type !== 'MemberExpression')
                    return;
                // Must be a method we care about
                if (!isSupportedMethod(node.argument))
                    return;
                const start = node.loc.start;
                const range = node.range;
                context.report({
                    fix: (fixer) => fixer.removeRange([range[0], range[0] + 6]),
                    loc: {
                        end: {
                            column: start.column + 5,
                            line: start.line,
                        },
                        start,
                    },
                    messageId: 'noUselessAwait',
                });
            },
        };
    },
    meta: {
        docs: {
            category: 'Possible Errors',
            description: 'Disallow unnecessary awaits for Playwright methods',
            recommended: true,
        },
        fixable: 'code',
        messages: {
            noUselessAwait: 'Unnecessary await expression. This method does not return a Promise.',
        },
        type: 'problem',
    },
};
