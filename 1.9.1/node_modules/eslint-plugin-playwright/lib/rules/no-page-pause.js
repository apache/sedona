"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../utils/ast");
exports.default = {
    create(context) {
        return {
            CallExpression(node) {
                if ((0, ast_1.isPageMethod)(node, 'pause')) {
                    context.report({ messageId: 'noPagePause', node });
                }
            },
        };
    },
    meta: {
        docs: {
            category: 'Possible Errors',
            description: 'Prevent usage of page.pause()',
            recommended: true,
        },
        messages: {
            noPagePause: 'Unexpected use of page.pause().',
        },
        type: 'problem',
    },
};
