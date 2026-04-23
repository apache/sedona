const scrollLock = require('../dist/scroll-lock');

test('page scroll disable/enable', () => {
    const $body = document.body;
    expect(scrollLock.getScrollState()).toBe(true);
    expect($body.style.overflow).toBe('');
    scrollLock.disablePageScroll();
    expect(scrollLock.getScrollState()).toBe(false);
    expect($body.style.overflow).toBe('hidden');
    scrollLock.enablePageScroll();
    expect(scrollLock.getScrollState()).toBe(true);
    expect($body.style.overflow).toBe('');
});

test('page scroll disable/enable with queue', () => {
    const $body = document.body;
    expect(scrollLock.getScrollState()).toBe(true);
    expect($body.style.overflow).toBe('');
    scrollLock.disablePageScroll();
    scrollLock.disablePageScroll();
    scrollLock.disablePageScroll();
    scrollLock.disablePageScroll();
    expect(scrollLock.getScrollState()).toBe(false);
    expect($body.style.overflow).toBe('hidden');
    scrollLock.enablePageScroll();
    expect(scrollLock.getScrollState()).toBe(false);
    expect($body.style.overflow).toBe('hidden');
    scrollLock.clearQueueScrollLocks();
    scrollLock.enablePageScroll();
    expect(scrollLock.getScrollState()).toBe(true);
    expect($body.style.overflow).toBe('');
});
