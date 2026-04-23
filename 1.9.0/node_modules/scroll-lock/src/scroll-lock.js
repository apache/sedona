import {
    eachNode,
    argumentAsArray,
    isElement,
    throwError,
    arrayAsSelector,
    findParentBySelector,
    elementScrollTopOnStart,
    elementScrollTopOnEnd,
    elementScrollLeftOnStart,
    elementScrollLeftOnEnd,
    elementIsScrollableField,
    elementHasSelector,
    elementIsInputRange,
} from './tools';

const FILL_GAP_AVAILABLE_METHODS = ['padding', 'margin', 'width', 'max-width', 'none'];
const TOUCH_DIRECTION_DETECT_OFFSET = 3;

const state = {
    scroll: true,
    queue: 0,
    scrollableSelectors: ['[data-scroll-lock-scrollable]'],
    lockableSelectors: ['body', '[data-scroll-lock-lockable]'],
    fillGapSelectors: ['body', '[data-scroll-lock-fill-gap]', '[data-scroll-lock-lockable]'],
    fillGapMethod: FILL_GAP_AVAILABLE_METHODS[0],
    //
    startTouchY: 0,
    startTouchX: 0,
};

export const disablePageScroll = (target) => {
    if (state.queue <= 0) {
        state.scroll = false;
        hideLockableOverflow();
        fillGaps();
    }

    addScrollableTarget(target);
    state.queue++;
};
export const enablePageScroll = (target) => {
    state.queue > 0 && state.queue--;
    if (state.queue <= 0) {
        state.scroll = true;
        showLockableOverflow();
        unfillGaps();
    }

    removeScrollableTarget(target);
};
export const getScrollState = () => {
    return state.scroll;
};
export const clearQueueScrollLocks = () => {
    state.queue = 0;
};
export const getTargetScrollBarWidth = ($target, onlyExists = false) => {
    if (isElement($target)) {
        const currentOverflowYProperty = $target.style.overflowY;
        if (onlyExists) {
            if (!getScrollState()) {
                $target.style.overflowY = $target.getAttribute('data-scroll-lock-saved-overflow-y-property');
            }
        } else {
            $target.style.overflowY = 'scroll';
        }
        const width = getCurrentTargetScrollBarWidth($target);
        $target.style.overflowY = currentOverflowYProperty;

        return width;
    } else {
        return 0;
    }
};
export const getCurrentTargetScrollBarWidth = ($target) => {
    if (isElement($target)) {
        if ($target === document.body) {
            const documentWidth = document.documentElement.clientWidth;
            const windowWidth = window.innerWidth;
            const currentWidth = windowWidth - documentWidth;

            return currentWidth;
        } else {
            const borderLeftWidthCurrentProperty = $target.style.borderLeftWidth;
            const borderRightWidthCurrentProperty = $target.style.borderRightWidth;
            $target.style.borderLeftWidth = '0px';
            $target.style.borderRightWidth = '0px';
            const currentWidth = $target.offsetWidth - $target.clientWidth;
            $target.style.borderLeftWidth = borderLeftWidthCurrentProperty;
            $target.style.borderRightWidth = borderRightWidthCurrentProperty;

            return currentWidth;
        }
    } else {
        return 0;
    }
};
export const getPageScrollBarWidth = (onlyExists = false) => {
    return getTargetScrollBarWidth(document.body, onlyExists);
};
export const getCurrentPageScrollBarWidth = () => {
    return getCurrentTargetScrollBarWidth(document.body);
};
export const addScrollableTarget = (target) => {
    if (target) {
        const targets = argumentAsArray(target);
        targets.map(($targets) => {
            eachNode($targets, ($target) => {
                if (isElement($target)) {
                    $target.setAttribute('data-scroll-lock-scrollable', '');
                } else {
                    throwError(`"${$target}" is not a Element.`);
                }
            });
        });
    }
};
export const removeScrollableTarget = (target) => {
    if (target) {
        const targets = argumentAsArray(target);
        targets.map(($targets) => {
            eachNode($targets, ($target) => {
                if (isElement($target)) {
                    $target.removeAttribute('data-scroll-lock-scrollable');
                } else {
                    throwError(`"${$target}" is not a Element.`);
                }
            });
        });
    }
};
export const addScrollableSelector = (selector) => {
    if (selector) {
        const selectors = argumentAsArray(selector);
        selectors.map((selector) => {
            state.scrollableSelectors.push(selector);
        });
    }
};
export const removeScrollableSelector = (selector) => {
    if (selector) {
        const selectors = argumentAsArray(selector);
        selectors.map((selector) => {
            state.scrollableSelectors = state.scrollableSelectors.filter((sSelector) => sSelector !== selector);
        });
    }
};
export const addLockableTarget = (target) => {
    if (target) {
        const targets = argumentAsArray(target);
        targets.map(($targets) => {
            eachNode($targets, ($target) => {
                if (isElement($target)) {
                    $target.setAttribute('data-scroll-lock-lockable', '');
                } else {
                    throwError(`"${$target}" is not a Element.`);
                }
            });
        });
        if (!getScrollState()) {
            hideLockableOverflow();
        }
    }
};
export const addLockableSelector = (selector) => {
    if (selector) {
        const selectors = argumentAsArray(selector);
        selectors.map((selector) => {
            state.lockableSelectors.push(selector);
        });
        if (!getScrollState()) {
            hideLockableOverflow();
        }
        addFillGapSelector(selector);
    }
};
export const setFillGapMethod = (method) => {
    if (method) {
        if (FILL_GAP_AVAILABLE_METHODS.indexOf(method) !== -1) {
            state.fillGapMethod = method;
            refillGaps();
        } else {
            const methods = FILL_GAP_AVAILABLE_METHODS.join(', ');
            throwError(`"${method}" method is not available!\nAvailable fill gap methods: ${methods}.`);
        }
    }
};
export const addFillGapTarget = (target) => {
    if (target) {
        const targets = argumentAsArray(target);
        targets.map(($targets) => {
            eachNode($targets, ($target) => {
                if (isElement($target)) {
                    $target.setAttribute('data-scroll-lock-fill-gap', '');
                    if (!state.scroll) {
                        fillGapTarget($target);
                    }
                } else {
                    throwError(`"${$target}" is not a Element.`);
                }
            });
        });
    }
};
export const removeFillGapTarget = (target) => {
    if (target) {
        const targets = argumentAsArray(target);
        targets.map(($targets) => {
            eachNode($targets, ($target) => {
                if (isElement($target)) {
                    $target.removeAttribute('data-scroll-lock-fill-gap');
                    if (!state.scroll) {
                        unfillGapTarget($target);
                    }
                } else {
                    throwError(`"${$target}" is not a Element.`);
                }
            });
        });
    }
};
export const addFillGapSelector = (selector) => {
    if (selector) {
        const selectors = argumentAsArray(selector);
        selectors.map((selector) => {
            if (state.fillGapSelectors.indexOf(selector) === -1) {
                state.fillGapSelectors.push(selector);
                if (!state.scroll) {
                    fillGapSelector(selector);
                }
            }
        });
    }
};
export const removeFillGapSelector = (selector) => {
    if (selector) {
        const selectors = argumentAsArray(selector);
        selectors.map((selector) => {
            state.fillGapSelectors = state.fillGapSelectors.filter((fSelector) => fSelector !== selector);
            if (!state.scroll) {
                unfillGapSelector(selector);
            }
        });
    }
};

export const refillGaps = () => {
    if (!state.scroll) {
        fillGaps();
    }
};

const hideLockableOverflow = () => {
    const selector = arrayAsSelector(state.lockableSelectors);
    hideLockableOverflowSelector(selector);
};
const showLockableOverflow = () => {
    const selector = arrayAsSelector(state.lockableSelectors);
    showLockableOverflowSelector(selector);
};
const hideLockableOverflowSelector = (selector) => {
    const $targets = document.querySelectorAll(selector);
    eachNode($targets, ($target) => {
        hideLockableOverflowTarget($target);
    });
};
const showLockableOverflowSelector = (selector) => {
    const $targets = document.querySelectorAll(selector);
    eachNode($targets, ($target) => {
        showLockableOverflowTarget($target);
    });
};
const hideLockableOverflowTarget = ($target) => {
    if (isElement($target) && $target.getAttribute('data-scroll-lock-locked') !== 'true') {
        const computedStyle = window.getComputedStyle($target);
        $target.setAttribute('data-scroll-lock-saved-overflow-y-property', computedStyle.overflowY);
        $target.setAttribute('data-scroll-lock-saved-inline-overflow-property', $target.style.overflow);
        $target.setAttribute('data-scroll-lock-saved-inline-overflow-y-property', $target.style.overflowY);

        $target.style.overflow = 'hidden';
        $target.setAttribute('data-scroll-lock-locked', 'true');
    }
};
const showLockableOverflowTarget = ($target) => {
    if (isElement($target) && $target.getAttribute('data-scroll-lock-locked') === 'true') {
        $target.style.overflow = $target.getAttribute('data-scroll-lock-saved-inline-overflow-property');
        $target.style.overflowY = $target.getAttribute('data-scroll-lock-saved-inline-overflow-y-property');

        $target.removeAttribute('data-scroll-lock-saved-overflow-property');
        $target.removeAttribute('data-scroll-lock-saved-inline-overflow-property');
        $target.removeAttribute('data-scroll-lock-saved-inline-overflow-y-property');
        $target.removeAttribute('data-scroll-lock-locked');
    }
};

const fillGaps = () => {
    state.fillGapSelectors.map((selector) => {
        fillGapSelector(selector);
    });
};
const unfillGaps = () => {
    state.fillGapSelectors.map((selector) => {
        unfillGapSelector(selector);
    });
};
const fillGapSelector = (selector) => {
    const $targets = document.querySelectorAll(selector);
    const isLockable = state.lockableSelectors.indexOf(selector) !== -1;
    eachNode($targets, ($target) => {
        fillGapTarget($target, isLockable);
    });
};
const fillGapTarget = ($target, isLockable = false) => {
    if (isElement($target)) {
        let scrollBarWidth;
        if ($target.getAttribute('data-scroll-lock-lockable') === '' || isLockable) {
            scrollBarWidth = getTargetScrollBarWidth($target, true);
        } else {
            const $lockableParent = findParentBySelector($target, arrayAsSelector(state.lockableSelectors));
            scrollBarWidth = getTargetScrollBarWidth($lockableParent, true);
        }

        if ($target.getAttribute('data-scroll-lock-filled-gap') === 'true') {
            unfillGapTarget($target);
        }

        const computedStyle = window.getComputedStyle($target);
        $target.setAttribute('data-scroll-lock-filled-gap', 'true');
        $target.setAttribute('data-scroll-lock-current-fill-gap-method', state.fillGapMethod);

        if (state.fillGapMethod === 'margin') {
            const currentMargin = parseFloat(computedStyle.marginRight);
            $target.style.marginRight = `${currentMargin + scrollBarWidth}px`;
        } else if (state.fillGapMethod === 'width') {
            $target.style.width = `calc(100% - ${scrollBarWidth}px)`;
        } else if (state.fillGapMethod === 'max-width') {
            $target.style.maxWidth = `calc(100% - ${scrollBarWidth}px)`;
        } else if (state.fillGapMethod === 'padding') {
            const currentPadding = parseFloat(computedStyle.paddingRight);
            $target.style.paddingRight = `${currentPadding + scrollBarWidth}px`;
        }
    }
};
const unfillGapSelector = (selector) => {
    const $targets = document.querySelectorAll(selector);
    eachNode($targets, ($target) => {
        unfillGapTarget($target);
    });
};
const unfillGapTarget = ($target) => {
    if (isElement($target)) {
        if ($target.getAttribute('data-scroll-lock-filled-gap') === 'true') {
            const currentFillGapMethod = $target.getAttribute('data-scroll-lock-current-fill-gap-method');
            $target.removeAttribute('data-scroll-lock-filled-gap');
            $target.removeAttribute('data-scroll-lock-current-fill-gap-method');

            if (currentFillGapMethod === 'margin') {
                $target.style.marginRight = ``;
            } else if (currentFillGapMethod === 'width') {
                $target.style.width = ``;
            } else if (currentFillGapMethod === 'max-width') {
                $target.style.maxWidth = ``;
            } else if (currentFillGapMethod === 'padding') {
                $target.style.paddingRight = ``;
            }
        }
    }
};

const onResize = (e) => {
    refillGaps();
};

const onTouchStart = (e) => {
    if (!state.scroll) {
        state.startTouchY = e.touches[0].clientY;
        state.startTouchX = e.touches[0].clientX;
    }
};
const onTouchMove = (e) => {
    if (!state.scroll) {
        const { startTouchY, startTouchX } = state;
        const currentClientY = e.touches[0].clientY;
        const currentClientX = e.touches[0].clientX;

        if (e.touches.length < 2) {
            const selector = arrayAsSelector(state.scrollableSelectors);
            const direction = {
                up: startTouchY < currentClientY,
                down: startTouchY > currentClientY,
                left: startTouchX < currentClientX,
                right: startTouchX > currentClientX,
            };
            const directionWithOffset = {
                up: startTouchY + TOUCH_DIRECTION_DETECT_OFFSET < currentClientY,
                down: startTouchY - TOUCH_DIRECTION_DETECT_OFFSET > currentClientY,
                left: startTouchX + TOUCH_DIRECTION_DETECT_OFFSET < currentClientX,
                right: startTouchX - TOUCH_DIRECTION_DETECT_OFFSET > currentClientX,
            };
            const handle = ($el, skip = false) => {
                if ($el) {
                    const parentScrollableEl = findParentBySelector($el, selector, false);
                    if (elementIsInputRange($el)) {
                        return false;
                    }

                    if (
                        skip ||
                        (elementIsScrollableField($el) && findParentBySelector($el, selector)) ||
                        elementHasSelector($el, selector)
                    ) {
                        let prevent = false;
                        if (elementScrollLeftOnStart($el) && elementScrollLeftOnEnd($el)) {
                            if (
                                (direction.up && elementScrollTopOnStart($el)) ||
                                (direction.down && elementScrollTopOnEnd($el))
                            ) {
                                prevent = true;
                            }
                        } else if (elementScrollTopOnStart($el) && elementScrollTopOnEnd($el)) {
                            if (
                                (direction.left && elementScrollLeftOnStart($el)) ||
                                (direction.right && elementScrollLeftOnEnd($el))
                            ) {
                                prevent = true;
                            }
                        } else if (
                            (directionWithOffset.up && elementScrollTopOnStart($el)) ||
                            (directionWithOffset.down && elementScrollTopOnEnd($el)) ||
                            (directionWithOffset.left && elementScrollLeftOnStart($el)) ||
                            (directionWithOffset.right && elementScrollLeftOnEnd($el))
                        ) {
                            prevent = true;
                        }
                        if (prevent) {
                            if (parentScrollableEl) {
                                handle(parentScrollableEl, true);
                            } else {
                                if (e.cancelable) {
                                    e.preventDefault();
                                }
                            }
                        }
                    } else {
                        handle(parentScrollableEl);
                    }
                } else {
                    if (e.cancelable) {
                        e.preventDefault();
                    }
                }
            };

            handle(e.target);
        }
    }
};
const onTouchEnd = (e) => {
    if (!state.scroll) {
        state.startTouchY = 0;
        state.startTouchX = 0;
    }
};

if (typeof window !== 'undefined') {
    window.addEventListener('resize', onResize);
}
if (typeof document !== 'undefined') {
    document.addEventListener('touchstart', onTouchStart);
    document.addEventListener('touchmove', onTouchMove, {
        passive: false,
    });
    document.addEventListener('touchend', onTouchEnd);
}

const deprecatedMethods = {
    hide(target) {
        throwError(
            '"hide" is deprecated! Use "disablePageScroll" instead. \n https://github.com/FL3NKEY/scroll-lock#disablepagescrollscrollabletarget'
        );

        disablePageScroll(target);
    },
    show(target) {
        throwError(
            '"show" is deprecated! Use "enablePageScroll" instead. \n https://github.com/FL3NKEY/scroll-lock#enablepagescrollscrollabletarget'
        );

        enablePageScroll(target);
    },
    toggle(target) {
        throwError('"toggle" is deprecated! Do not use it.');

        if (getScrollState()) {
            disablePageScroll();
        } else {
            enablePageScroll(target);
        }
    },
    getState() {
        throwError(
            '"getState" is deprecated! Use "getScrollState" instead. \n https://github.com/FL3NKEY/scroll-lock#getscrollstate'
        );

        return getScrollState();
    },
    getWidth() {
        throwError(
            '"getWidth" is deprecated! Use "getPageScrollBarWidth" instead. \n https://github.com/FL3NKEY/scroll-lock#getpagescrollbarwidth'
        );

        return getPageScrollBarWidth();
    },
    getCurrentWidth() {
        throwError(
            '"getCurrentWidth" is deprecated! Use "getCurrentPageScrollBarWidth" instead. \n https://github.com/FL3NKEY/scroll-lock#getcurrentpagescrollbarwidth'
        );

        return getCurrentPageScrollBarWidth();
    },
    setScrollableTargets(target) {
        throwError(
            '"setScrollableTargets" is deprecated! Use "addScrollableTarget" instead. \n https://github.com/FL3NKEY/scroll-lock#addscrollabletargetscrollabletarget'
        );

        addScrollableTarget(target);
    },
    setFillGapSelectors(selector) {
        throwError(
            '"setFillGapSelectors" is deprecated! Use "addFillGapSelector" instead. \n https://github.com/FL3NKEY/scroll-lock#addfillgapselectorfillgapselector'
        );

        addFillGapSelector(selector);
    },
    setFillGapTargets(target) {
        throwError(
            '"setFillGapTargets" is deprecated! Use "addFillGapTarget" instead. \n https://github.com/FL3NKEY/scroll-lock#addfillgaptargetfillgaptarget'
        );

        addFillGapTarget(target);
    },
    clearQueue() {
        throwError(
            '"clearQueue" is deprecated! Use "clearQueueScrollLocks" instead. \n https://github.com/FL3NKEY/scroll-lock#clearqueuescrolllocks'
        );

        clearQueueScrollLocks();
    },
};

const scrollLock = {
    disablePageScroll,
    enablePageScroll,

    getScrollState,
    clearQueueScrollLocks,
    getTargetScrollBarWidth,
    getCurrentTargetScrollBarWidth,
    getPageScrollBarWidth,
    getCurrentPageScrollBarWidth,

    addScrollableSelector,
    removeScrollableSelector,

    addScrollableTarget,
    removeScrollableTarget,

    addLockableSelector,

    addLockableTarget,

    addFillGapSelector,
    removeFillGapSelector,

    addFillGapTarget,
    removeFillGapTarget,

    setFillGapMethod,
    refillGaps,

    _state: state,

    ...deprecatedMethods,
};

export default scrollLock;
