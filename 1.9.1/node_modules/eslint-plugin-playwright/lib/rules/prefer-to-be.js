"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const fixer_1 = require("../utils/fixer");
const parseExpectCall_1 = require("../utils/parseExpectCall");
function shouldUseToBe(expectCall) {
    let arg = expectCall.args[0];
    if (arg.type === 'UnaryExpression' && arg.operator === '-') {
        arg = arg.argument;
    }
    if (arg.type === 'Literal') {
        // regex literals are classed as literals, but they're actually objects
        // which means "toBe" will give different results than other matchers
        return !('regex' in arg);
    }
    return arg.type === 'TemplateLiteral';
}
function reportPreferToBe(context, expectCall, whatToBe, notModifier) {
    context.report({
        fix(fixer) {
            const fixes = [
                (0, fixer_1.replaceAccessorFixer)(fixer, expectCall.matcher, `toBe${whatToBe}`),
            ];
            if (expectCall.args?.length && whatToBe !== '') {
                fixes.push(fixer.remove(expectCall.args[0]));
            }
            if (notModifier) {
                const [start, end] = notModifier.range;
                fixes.push(fixer.removeRange([start - 1, end]));
            }
            return fixes;
        },
        messageId: `useToBe${whatToBe}`,
        node: expectCall.matcher,
    });
}
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                const expectCall = (0, parseExpectCall_1.parseExpectCall)(node);
                if (!expectCall)
                    return;
                const notMatchers = ['toBeUndefined', 'toBeDefined'];
                const notModifier = expectCall.modifiers.find((node) => (0, ast_1.getStringValue)(node) === 'not');
                if (notModifier && notMatchers.includes(expectCall.matcherName)) {
                    return reportPreferToBe(context, expectCall, expectCall.matcherName === 'toBeDefined' ? 'Undefined' : 'Defined', notModifier);
                }
                const argumentMatchers = ['toBe', 'toEqual', 'toStrictEqual'];
                const firstArg = expectCall.args[0];
                if (!argumentMatchers.includes(expectCall.matcherName) || !firstArg) {
                    return;
                }
                if (firstArg.type === 'Literal' && firstArg.value === null) {
                    return reportPreferToBe(context, expectCall, 'Null');
                }
                if ((0, ast_1.isIdentifier)(firstArg, 'undefined')) {
                    const name = notModifier ? 'Defined' : 'Undefined';
                    return reportPreferToBe(context, expectCall, name, notModifier);
                }
                if ((0, ast_1.isIdentifier)(firstArg, 'NaN')) {
                    return reportPreferToBe(context, expectCall, 'NaN');
                }
                if (shouldUseToBe(expectCall) && expectCall.matcherName !== 'toBe') {
                    reportPreferToBe(context, expectCall, '');
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Suggest using `toBe()` for primitive literals',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/prefer-to-be.md',
        },
        fixable: 'code',
        messages: {
            useToBe: 'Use `toBe` when expecting primitive literals',
            useToBeDefined: 'Use `toBeDefined` instead',
            useToBeNaN: 'Use `toBeNaN` instead',
            useToBeNull: 'Use `toBeNull` instead',
            useToBeUndefined: 'Use `toBeUndefined` instead',
        },
        schema: [],
        type: 'suggestion',
    },
};
