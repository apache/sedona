export const argumentAsArray = (argument) => (Array.isArray(argument) ? argument : [argument]);
export const isElement = (target) => target instanceof Node;
export const isElementList = (nodeList) => nodeList instanceof NodeList;
export const eachNode = (nodeList, callback) => {
    if (nodeList && callback) {
        nodeList = isElementList(nodeList) ? nodeList : [nodeList];
        for (let i = 0; i < nodeList.length; i++) {
            if (callback(nodeList[i], i, nodeList.length) === true) {
                break;
            }
        }
    }
};
export const throwError = (message) => console.error(`[scroll-lock] ${message}`);
export const arrayAsSelector = (array) => {
    if (Array.isArray(array)) {
        const selector = array.join(', ');
        return selector;
    }
};
export const nodeListAsArray = (nodeList) => {
    const nodes = [];
    eachNode(nodeList, (node) => nodes.push(node));

    return nodes;
};
export const findParentBySelector = ($el, selector, self = true, $root = document) => {
    if (self && nodeListAsArray($root.querySelectorAll(selector)).indexOf($el) !== -1) {
        return $el;
    }

    while (($el = $el.parentElement) && nodeListAsArray($root.querySelectorAll(selector)).indexOf($el) === -1);
    return $el;
};
export const elementHasSelector = ($el, selector, $root = document) => {
    const has = nodeListAsArray($root.querySelectorAll(selector)).indexOf($el) !== -1;
    return has;
};
export const elementHasOverflowHidden = ($el) => {
    if ($el) {
        const computedStyle = getComputedStyle($el);
        const overflowIsHidden = computedStyle.overflow === 'hidden';
        return overflowIsHidden;
    }
};
export const elementScrollTopOnStart = ($el) => {
    if ($el) {
        if (elementHasOverflowHidden($el)) {
            return true;
        }

        const scrollTop = $el.scrollTop;
        return scrollTop <= 0;
    }
};
export const elementScrollTopOnEnd = ($el) => {
    if ($el) {
        if (elementHasOverflowHidden($el)) {
            return true;
        }

        const scrollTop = $el.scrollTop;
        const scrollHeight = $el.scrollHeight;
        const scrollTopWithHeight = scrollTop + $el.offsetHeight;
        return scrollTopWithHeight >= scrollHeight;
    }
};
export const elementScrollLeftOnStart = ($el) => {
    if ($el) {
        if (elementHasOverflowHidden($el)) {
            return true;
        }

        const scrollLeft = $el.scrollLeft;
        return scrollLeft <= 0;
    }
};
export const elementScrollLeftOnEnd = ($el) => {
    if ($el) {
        if (elementHasOverflowHidden($el)) {
            return true;
        }

        const scrollLeft = $el.scrollLeft;
        const scrollWidth = $el.scrollWidth;
        const scrollLeftWithWidth = scrollLeft + $el.offsetWidth;
        return scrollLeftWithWidth >= scrollWidth;
    }
};
export const elementIsScrollableField = ($el) => {
    const selector = 'textarea, [contenteditable="true"]';
    return elementHasSelector($el, selector);
};
export const elementIsInputRange = ($el) => {
    const selector = 'input[type="range"]';
    return elementHasSelector($el, selector);
};
