"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const parseExpectCall_1 = require("../utils/parseExpectCall");
exports.default = {
    create(context) {
        const restrictedChains = (context.options?.[0] ?? {});
        return {
            CallExpression(node) {
                const expectCall = (0, parseExpectCall_1.parseExpectCall)(node);
                if (!expectCall)
                    return;
                Object.entries(restrictedChains)
                    .map(([restriction, message]) => {
                    const chain = expectCall.members;
                    const restrictionLinks = restriction.split('.').length;
                    // Find in the full chain, where the restriction chain starts
                    const startIndex = chain.findIndex((_, i) => {
                        // Construct the partial chain to compare against the restriction
                        // chain string.
                        const partial = chain
                            .slice(i, i + restrictionLinks)
                            .map(ast_1.getStringValue)
                            .join('.');
                        return partial === restriction;
                    });
                    return {
                        // If the restriction chain was found, return the portion of the
                        // chain that matches the restriction chain.
                        chain: startIndex !== -1
                            ? chain.slice(startIndex, startIndex + restrictionLinks)
                            : [],
                        message,
                        restriction,
                    };
                })
                    .filter(({ chain }) => chain.length)
                    .forEach(({ chain, message, restriction }) => {
                    context.report({
                        data: { message: message ?? '', restriction },
                        loc: {
                            end: chain[chain.length - 1].loc.end,
                            start: chain[0].loc.start,
                        },
                        messageId: message ? 'restrictedWithMessage' : 'restricted',
                    });
                });
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Disallow specific matchers & modifiers',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/no-restricted-matchers.md',
        },
        messages: {
            restricted: 'Use of `{{restriction}}` is disallowed',
            restrictedWithMessage: '{{message}}',
        },
        schema: [
            {
                additionalProperties: {
                    type: ['string', 'null'],
                },
                type: 'object',
            },
        ],
        type: 'suggestion',
    },
};
