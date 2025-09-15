"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isPageMethod = exports.getMatchers = exports.isExpectCall = exports.getExpectType = exports.isTestHook = exports.isTest = exports.findParent = exports.isDescribeCall = exports.isTestIdentifier = exports.isPropertyAccessor = exports.isBooleanLiteral = exports.isStringLiteral = exports.isIdentifier = exports.getRawValue = exports.getStringValue = void 0;
function getStringValue(node) {
    if (!node)
        return '';
    return node.type === 'Identifier'
        ? node.name
        : node.type === 'TemplateLiteral'
            ? node.quasis[0].value.raw
            : node.type === 'Literal' && typeof node.value === 'string'
                ? node.value
                : '';
}
exports.getStringValue = getStringValue;
function getRawValue(node) {
    return node.type === 'Literal' ? node.raw : undefined;
}
exports.getRawValue = getRawValue;
function isIdentifier(node, name) {
    return (node.type === 'Identifier' &&
        (typeof name === 'string' ? node.name === name : name.test(node.name)));
}
exports.isIdentifier = isIdentifier;
function isLiteral(node, type, value) {
    return (node.type === 'Literal' &&
        (value === undefined
            ? typeof node.value === type
            : node.value === value));
}
function isStringLiteral(node, value) {
    return isLiteral(node, 'string', value);
}
exports.isStringLiteral = isStringLiteral;
function isBooleanLiteral(node, value) {
    return isLiteral(node, 'boolean', value);
}
exports.isBooleanLiteral = isBooleanLiteral;
function isPropertyAccessor(node, name) {
    return getStringValue(node.property) === name;
}
exports.isPropertyAccessor = isPropertyAccessor;
function isTestIdentifier(node) {
    return (isIdentifier(node, 'test') ||
        (node.type === 'MemberExpression' && isIdentifier(node.object, 'test')));
}
exports.isTestIdentifier = isTestIdentifier;
const describeProperties = new Set([
    'parallel',
    'serial',
    'only',
    'skip',
    'fixme',
]);
function isDescribeCall(node) {
    const inner = node.type === 'CallExpression' ? node.callee : node;
    // Allow describe without test prefix
    if (isIdentifier(inner, 'describe')) {
        return true;
    }
    if (inner.type !== 'MemberExpression') {
        return false;
    }
    return isPropertyAccessor(inner, 'describe')
        ? true
        : describeProperties.has(getStringValue(inner.property))
            ? isDescribeCall(inner.object)
            : false;
}
exports.isDescribeCall = isDescribeCall;
function findParent(node, type) {
    if (!node.parent)
        return;
    return node.parent.type === type
        ? node.parent
        : findParent(node.parent, type);
}
exports.findParent = findParent;
function isTest(node, modifiers) {
    return (isTestIdentifier(node.callee) &&
        !isDescribeCall(node) &&
        (node.callee.type !== 'MemberExpression' ||
            !modifiers ||
            modifiers?.includes(getStringValue(node.callee.property))) &&
        node.arguments.length === 2 &&
        ['ArrowFunctionExpression', 'FunctionExpression'].includes(node.arguments[1].type));
}
exports.isTest = isTest;
const testHooks = new Set(['afterAll', 'afterEach', 'beforeAll', 'beforeEach']);
function isTestHook(node) {
    return (node.callee.type === 'MemberExpression' &&
        isIdentifier(node.callee.object, 'test') &&
        testHooks.has(getStringValue(node.callee.property)));
}
exports.isTestHook = isTestHook;
const expectSubCommands = new Set(['soft', 'poll']);
function getExpectType(node) {
    if (isIdentifier(node.callee, /(^expect|Expect)$/)) {
        return 'standalone';
    }
    if (node.callee.type === 'MemberExpression' &&
        isIdentifier(node.callee.object, 'expect')) {
        const type = getStringValue(node.callee.property);
        return expectSubCommands.has(type) ? type : undefined;
    }
}
exports.getExpectType = getExpectType;
function isExpectCall(node) {
    return !!getExpectType(node);
}
exports.isExpectCall = isExpectCall;
function getMatchers(node, chain = []) {
    if (node.parent.type === 'MemberExpression' && node.parent.object === node) {
        return getMatchers(node.parent, [
            ...chain,
            node.parent.property,
        ]);
    }
    return chain;
}
exports.getMatchers = getMatchers;
/**
 * Digs through a series of MemberExpressions and CallExpressions to find an
 * Identifier with the given name.
 */
function dig(node, identifier) {
    return node.type === 'MemberExpression'
        ? dig(node.property, identifier)
        : node.type === 'CallExpression'
            ? dig(node.callee, identifier)
            : node.type === 'Identifier'
                ? isIdentifier(node, identifier)
                : false;
}
function isPageMethod(node, name) {
    return (node.callee.type === 'MemberExpression' &&
        dig(node.callee.object, /(^(page|frame)|(Page|Frame)$)/) &&
        isPropertyAccessor(node.callee, name));
}
exports.isPageMethod = isPageMethod;
