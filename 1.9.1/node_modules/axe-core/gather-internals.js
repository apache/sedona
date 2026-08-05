(() => {
var elementInternalsMap = (() => {
  var __commonJS = (cb, mod) => () => (mod || cb((mod = {exports: {}}).exports, mod), mod.exports);

  // lib/gather-internals/main.js
  var require_main = __commonJS((exports, module) => {
    walkTree();
    module.exports = elementInternalsMap;
  });

  // lib/core/utils/is-valid-custom-element-name.js
  var reservedNames = [
    "annotation-xml",
    "color-profile",
    "font-face",
    "font-face-src",
    "font-face-uri",
    "font-face-format",
    "font-face-name",
    "missing-glyph"
  ];
  var validLocalNameRegex = /^(?:[A-Za-z][^\0\t\n\f\r\u0020/>]*|[:_\u0080-\u{10FFFF}][A-Za-z0-9-.:_\u0080-\u{10FFFF}]*)$/u;
  var alphaLowerRegex = /[a-z]/;
  var alphaUpperRegex = /[A-Z]/;
  function isValidCustomElementName(nodeName) {
    return !reservedNames.includes(nodeName) && validLocalNameRegex.test(nodeName) && alphaLowerRegex.test(nodeName[0]) && !alphaUpperRegex.test(nodeName) && nodeName.includes("-");
  }

  // lib/core/utils/get-element-internals.js
  var propNames = ["_internals", "internals", "internals_"];
  var symbolNames = ["internals", "privateInternals"];
  function getElementInternals(node) {
    if (!isValidCustomElementName(node.nodeName.toLowerCase())) {
      return;
    }
    const mapInternals = globalThis._elementInternals?.get(node);
    if (mapInternals) {
      return mapInternals;
    }
    if (!("ElementInternals" in window)) {
      return;
    }
    for (const propName of propNames) {
      if (Object.getOwnPropertyDescriptor(node, propName)?.get) {
        continue;
      }
      if (node[propName] instanceof window.ElementInternals) {
        return node[propName];
      }
    }
    const ownSymbols = Object.getOwnPropertySymbols(node);
    if (!ownSymbols.length) {
      return;
    }
    for (const symbolName of symbolNames) {
      const symbol = ownSymbols.find((s) => s.description === symbolName);
      if (symbol) {
        if (Object.getOwnPropertyDescriptor(node, symbol)?.get) {
          continue;
        }
        if (node[symbol] instanceof window.ElementInternals) {
          return node[symbol];
        }
      }
    }
  }

  // lib/core/utils/escape-selector.js
  function escapeSelector(value) {
    const string = String(value);
    const length = string.length;
    let index = -1;
    let codeUnit;
    let result = "";
    const firstCodeUnit = string.charCodeAt(0);
    while (++index < length) {
      codeUnit = string.charCodeAt(index);
      if (codeUnit == 0) {
        result += "\uFFFD";
        continue;
      }
      if (codeUnit >= 1 && codeUnit <= 31 || codeUnit == 127 || index == 0 && codeUnit >= 48 && codeUnit <= 57 || index == 1 && codeUnit >= 48 && codeUnit <= 57 && firstCodeUnit == 45) {
        result += "\\" + codeUnit.toString(16) + " ";
        continue;
      }
      if (index == 0 && length == 1 && codeUnit == 45) {
        result += "\\" + string.charAt(index);
        continue;
      }
      if (codeUnit >= 128 || codeUnit == 45 || codeUnit == 95 || codeUnit >= 48 && codeUnit <= 57 || codeUnit >= 65 && codeUnit <= 90 || codeUnit >= 97 && codeUnit <= 122) {
        result += string.charAt(index);
        continue;
      }
      result += "\\" + string.charAt(index);
    }
    return result;
  }
  var escape_selector_default = escapeSelector;

  // lib/core/utils/get-shadow-selector.js
  function getShadowSelector(generateSelector, elm, options = {}) {
    if (!elm) {
      return "";
    }
    let doc = elm.getRootNode && elm.getRootNode() || document;
    if (doc.nodeType !== 11) {
      return generateSelector(elm, options, doc);
    }
    const stack = [];
    while (doc.nodeType === 11) {
      if (!doc.host) {
        return "";
      }
      stack.unshift({elm, doc});
      elm = doc.host;
      doc = elm.getRootNode();
    }
    stack.unshift({elm, doc});
    return stack.map((item) => generateSelector(item.elm, options, item.doc));
  }

  // lib/core/utils/get-ancestry.js
  function generateAncestry(node) {
    const nodeName = escape_selector_default(node.nodeName.toLowerCase());
    const parentElement = node.parentElement;
    const parentNode = node.parentNode;
    let nthChild = "";
    if (nodeName !== "head" && nodeName !== "body" && parentNode?.children.length > 1) {
      const index = Array.prototype.indexOf.call(parentNode.children, node) + 1;
      nthChild = `:nth-child(${index})`;
    }
    if (!parentElement) {
      return nodeName + nthChild;
    }
    return generateAncestry(parentElement) + " > " + nodeName + nthChild;
  }
  function getAncestry(elm, options) {
    return getShadowSelector(generateAncestry, elm, options);
  }

  // lib/core/utils/is-shadow-root.js
  var possibleShadowRoots = [
    "article",
    "aside",
    "blockquote",
    "body",
    "div",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "main",
    "nav",
    "p",
    "section",
    "span"
  ];
  function isShadowRoot(node) {
    if (node.shadowRoot) {
      const nodeName = node.nodeName.toLowerCase();
      if (possibleShadowRoots.includes(nodeName) || isValidCustomElementName(nodeName)) {
        return true;
      }
    }
    return false;
  }
  var is_shadow_root_default = isShadowRoot;

  // lib/gather-internals/walk-tree.js
  var elementInternalsMap = [];
  var ariaPropRegex = /^aria[A-Z]/;
  var propsToCapture = ["role", "labels", "form"];
  function walkTree(tree = document.body) {
    const treeWalker = document.createTreeWalker(tree, globalThis.NodeFilter.SHOW_ELEMENT, null, false);
    let node = treeWalker.currentNode;
    while (node) {
      const elementInternals = getElementInternals(node);
      if (elementInternals) {
        const ancestry = getAncestry(node);
        const internals = {};
        for (const prop in elementInternals) {
          if (!ariaPropRegex.test(prop) && !propsToCapture.includes(prop)) {
            continue;
          }
          try {
            const value = elementInternals[prop];
            if (value === null) {
              continue;
            }
            if (value instanceof globalThis.HTMLElement) {
              internals[prop] = value.isConnected ? {type: "HTMLElement", value: getAncestry(value)} : void 0;
            } else if (Array.isArray(value) || value instanceof globalThis.NodeList) {
              const array = Array.from(value).filter((n) => n.isConnected);
              internals[prop] = array.length ? {type: "NodeList", value: array.map((n) => getAncestry(n))} : void 0;
            } else if (typeof value === "string") {
              internals[prop] = value;
            }
          } catch {
          }
        }
        elementInternalsMap.push({
          ancestry,
          internals
        });
      }
      if (is_shadow_root_default(node)) {
        walkTree(node.shadowRoot);
      }
      node = treeWalker.nextNode();
    }
  }
  return require_main();
})();
return elementInternalsMap;
})();
