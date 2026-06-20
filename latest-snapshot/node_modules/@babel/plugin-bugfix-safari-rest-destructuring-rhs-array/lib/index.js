"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _helperPluginUtils = require("@babel/helper-plugin-utils");
var _util = require("./util.js");
var _default = exports.default = (0, _helperPluginUtils.declare)(api => {
  const {
    types: t,
    assertVersion
  } = api;
  assertVersion(7);
  return {
    name: "plugin-bugfix-safari-rest-destructuring-rhs-array",
    visitor: {
      ArrayPattern(path) {
        const rhs = (0, _util.shouldTransform)(path);
        if (rhs) {
          path.replaceWith(t.arrayPattern([path.node]));
          rhs.replaceWith(t.arrayExpression([rhs.node]));
        }
      }
    }
  };
});

//# sourceMappingURL=index.js.map
