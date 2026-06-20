"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isPathSimpleArrayWithLength = isPathSimpleArrayWithLength;
exports.shouldTransform = shouldTransform;
var _helperSkipTransparentExpressionWrappers = require("@babel/helper-skip-transparent-expression-wrappers");
function isPathSimpleArrayWithLength(path, N) {
  var _path$node$extra;
  if (!path.isArrayExpression()) {
    return false;
  }
  const elementPaths = path.get("elements");
  return elementPaths.length === N && elementPaths.every(elementPath => elementPath.node && !elementPath.isSpreadElement()) && !((_path$node$extra = path.node.extra) != null && _path$node$extra.trailingComma);
}
function shouldTransform(path) {
  const elementPaths = path.get("elements");
  const elementPathsLength = elementPaths.length;
  if (elementPaths[elementPathsLength - 1].isRestElement()) {
    const {
      parentPath
    } = path;
    const rhsPath = parentPath.isVariableDeclarator() ? parentPath.get("init") : parentPath.isAssignmentExpression() ? parentPath.get("right") : null;
    if (rhsPath != null && rhsPath.node && isPathSimpleArrayWithLength((0, _helperSkipTransparentExpressionWrappers.skipTransparentExprWrappers)(rhsPath), elementPathsLength)) {
      return rhsPath;
    }
  }
  return false;
}

//# sourceMappingURL=util.js.map
