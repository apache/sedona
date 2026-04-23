"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
function isString(node) {
    return node && ((0, ast_1.isStringLiteral)(node) || node.type === 'TemplateLiteral');
}
exports.default = {
    create(context) {
        const { allowedPrefixes, ignore, ignoreTopLevelDescribe } = {
            allowedPrefixes: [],
            ignore: [],
            ignoreTopLevelDescribe: false,
            ...(context.options?.[0] ?? {}),
        };
        let describeCount = 0;
        return {
            CallExpression(node) {
                const method = (0, ast_1.isDescribeCall)(node)
                    ? 'test.describe'
                    : (0, ast_1.isTest)(node)
                        ? 'test'
                        : null;
                if (method === 'test.describe') {
                    describeCount++;
                    if (ignoreTopLevelDescribe && describeCount === 1) {
                        return;
                    }
                }
                else if (!method) {
                    return;
                }
                const [title] = node.arguments;
                if (!isString(title)) {
                    return;
                }
                const description = (0, ast_1.getStringValue)(title);
                if (!description ||
                    allowedPrefixes.some((name) => description.startsWith(name))) {
                    return;
                }
                const firstCharacter = description.charAt(0);
                if (!firstCharacter ||
                    firstCharacter === firstCharacter.toLowerCase() ||
                    ignore.includes(method)) {
                    return;
                }
                context.report({
                    data: { method },
                    fix(fixer) {
                        const rangeIgnoringQuotes = [
                            title.range[0] + 1,
                            title.range[1] - 1,
                        ];
                        const newDescription = description.substring(0, 1).toLowerCase() +
                            description.substring(1);
                        return fixer.replaceTextRange(rangeIgnoringQuotes, newDescription);
                    },
                    messageId: 'unexpectedLowercase',
                    node: node.arguments[0],
                });
            },
            'CallExpression:exit'(node) {
                if ((0, ast_1.isDescribeCall)(node)) {
                    describeCount--;
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Enforce lowercase test names',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/prefer-lowercase-title.md',
        },
        fixable: 'code',
        messages: {
            unexpectedLowercase: '`{{method}}`s should begin with lowercase',
        },
        schema: [
            {
                additionalProperties: false,
                properties: {
                    allowedPrefixes: {
                        additionalItems: false,
                        items: { type: 'string' },
                        type: 'array',
                    },
                    ignore: {
                        additionalItems: false,
                        items: {
                            enum: ['test.describe', 'test'],
                        },
                        type: 'array',
                    },
                    ignoreTopLevelDescribe: {
                        default: false,
                        type: 'boolean',
                    },
                },
                type: 'object',
            },
        ],
        type: 'suggestion',
    },
};
