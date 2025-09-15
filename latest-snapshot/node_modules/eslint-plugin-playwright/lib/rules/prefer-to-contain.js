"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
const parseExpectCall_1 = require("../utils/parseExpectCall");
const matchers = new Set(['toBe', 'toEqual', 'toStrictEqual']);
const isFixableIncludesCallExpression = (node) => node.type === 'CallExpression' &&
    node.callee.type === 'MemberExpression' &&
    (0, ast_1.isPropertyAccessor)(node.callee, 'includes') &&
    node.arguments.length === 1 &&
    node.arguments[0].type !== 'SpreadElement';
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                const expectCall = (0, parseExpectCall_1.parseExpectCall)(node);
                if (!expectCall || expectCall.args.length === 0)
                    return;
                const { args, matcher, matcherName } = expectCall;
                const [includesCall] = node.arguments;
                const [matcherArg] = args;
                if (!includesCall ||
                    matcherArg.type === 'SpreadElement' ||
                    !matchers.has(matcherName) ||
                    !(0, ast_1.isBooleanLiteral)(matcherArg) ||
                    !isFixableIncludesCallExpression(includesCall)) {
                    return;
                }
                const notModifier = expectCall.modifiers.find((node) => (0, ast_1.getStringValue)(node) === 'not');
                context.report({
                    fix(fixer) {
                        const sourceCode = context.getSourceCode();
                        // We need to negate the expectation if the current expected
                        // value is itself negated by the "not" modifier
                        const addNotModifier = matcherArg.type === 'Literal' &&
                            matcherArg.value === !!notModifier;
                        const fixes = [
                            // remove the "includes" call entirely
                            fixer.removeRange([
                                includesCall.callee.property.range[0] - 1,
                                includesCall.range[1],
                            ]),
                            // replace the current matcher with "toContain", adding "not" if needed
                            fixer.replaceText(matcher, addNotModifier ? 'not.toContain' : 'toContain'),
                            // replace the matcher argument with the value from the "includes"
                            fixer.replaceText(expectCall.args[0], sourceCode.getText(includesCall.arguments[0])),
                        ];
                        // Remove the "not" modifier if needed
                        if (notModifier) {
                            fixes.push(fixer.removeRange([
                                notModifier.range[0],
                                notModifier.range[1] + 1,
                            ]));
                        }
                        return fixes;
                    },
                    messageId: 'useToContain',
                    node: matcher,
                });
            },
        };
    },
    meta: {
        docs: {
            category: 'Best Practices',
            description: 'Suggest using toContain()',
            recommended: false,
            url: 'https://github.com/playwright-community/eslint-plugin-playwright/tree/main/docs/rules/prefer-to-contain.md',
        },
        fixable: 'code',
        messages: {
            useToContain: 'Use toContain() instead',
        },
        type: 'suggestion',
    },
};
