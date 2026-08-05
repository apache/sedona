const scrollLock = require('../dist/scroll-lock');

test('add fill gap selector', () => {
    document.body.innerHTML = `		
        <div class="fill-gap-selector"></div>
        <div class="fill-gap-selector-1"></div>
        <div class="fill-gap-selector-2"></div>
    `;

    scrollLock.disablePageScroll();

    const initialFillGapSelectors = JSON.parse(JSON.stringify(scrollLock._state.fillGapSelectors));
    const $fillGapSelector = document.querySelector('.fill-gap-selector');
    expect($fillGapSelector.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);
    scrollLock.addFillGapSelector('.fill-gap-selector');
    initialFillGapSelectors.push('.fill-gap-selector');
    expect(scrollLock._state.fillGapSelectors).toEqual(initialFillGapSelectors);
    expect($fillGapSelector.getAttribute('data-scroll-lock-filled-gap')).toEqual('true');

    const $fillGapSelector1 = document.querySelector('.fill-gap-selector-1');
    const $fillGapSelector2 = document.querySelector('.fill-gap-selector-2');
    expect($fillGapSelector1.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);
    expect($fillGapSelector2.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);
    scrollLock.addFillGapSelector(['.fill-gap-selector-1', '.fill-gap-selector-2']);
    initialFillGapSelectors.push('.fill-gap-selector-1');
    initialFillGapSelectors.push('.fill-gap-selector-2');
    expect(scrollLock._state.fillGapSelectors).toEqual(initialFillGapSelectors);
    expect($fillGapSelector1.getAttribute('data-scroll-lock-filled-gap')).toEqual('true');
    expect($fillGapSelector2.getAttribute('data-scroll-lock-filled-gap')).toEqual('true');

    scrollLock.enablePageScroll();
    expect($fillGapSelector1.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);
    expect($fillGapSelector2.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);
});

test('remove fill gap selector', () => {
    scrollLock.disablePageScroll();

    let initialFillGapSelectors = JSON.parse(JSON.stringify(scrollLock._state.fillGapSelectors));
    const $fillGapSelector = document.querySelector('.fill-gap-selector');
    expect($fillGapSelector.getAttribute('data-scroll-lock-filled-gap')).toEqual('true');
    scrollLock.removeFillGapSelector('.fill-gap-selector');
    initialFillGapSelectors = initialFillGapSelectors.filter((s) => s !== '.fill-gap-selector');
    expect(scrollLock._state.fillGapSelectors).toEqual(initialFillGapSelectors);
    expect($fillGapSelector.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);

    const $fillGapSelector1 = document.querySelector('.fill-gap-selector-1');
    const $fillGapSelector2 = document.querySelector('.fill-gap-selector-2');
    expect($fillGapSelector1.getAttribute('data-scroll-lock-filled-gap')).toEqual('true');
    expect($fillGapSelector2.getAttribute('data-scroll-lock-filled-gap')).toEqual('true');
    scrollLock.removeFillGapSelector(['.fill-gap-selector-1', '.fill-gap-selector-2']);
    initialFillGapSelectors = initialFillGapSelectors.filter((s) => s !== '.fill-gap-selector-1');
    initialFillGapSelectors = initialFillGapSelectors.filter((s) => s !== '.fill-gap-selector-2');
    expect(scrollLock._state.fillGapSelectors).toEqual(initialFillGapSelectors);
    expect($fillGapSelector1.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);
    expect($fillGapSelector2.getAttribute('data-scroll-lock-filled-gap')).toEqual(null);

    scrollLock.enablePageScroll();
});

test('add fill gap target', () => {
    document.body.innerHTML = `
        <div id="fill-gap-target"></div>

        <div class="fill-gap-target"></div>
        <div class="fill-gap-target"></div>
        <div class="fill-gap-target"></div>
    `;

    const $fillGapTarget = document.querySelector('#fill-gap-target');
    expect($fillGapTarget.getAttribute('data-scroll-lock-fill-gap')).toBe(null);
    scrollLock.addFillGapTarget($fillGapTarget);
    expect($fillGapTarget.getAttribute('data-scroll-lock-fill-gap')).toBe('');
    expect($fillGapTarget.getAttribute('data-scroll-lock-filled-gap')).toBe(null);
    scrollLock.disablePageScroll();
    expect($fillGapTarget.getAttribute('data-scroll-lock-filled-gap')).toBe('true');
    scrollLock.enablePageScroll();
    expect($fillGapTarget.getAttribute('data-scroll-lock-filled-gap')).toBe(null);

    const $fillGapTargets = document.querySelectorAll('.fill-gap-target');
    for (let i = 0; i < $fillGapTargets.length; i++) {
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-fill-gap')).toBe(null);
    }
    scrollLock.addFillGapTarget($fillGapTargets);
    for (let i = 0; i < $fillGapTargets.length; i++) {
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-fill-gap')).toBe('');
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-filled-gap')).toBe(null);
    }
    scrollLock.disablePageScroll();
    for (let i = 0; i < $fillGapTargets.length; i++) {
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-filled-gap')).toBe('true');
    }
    scrollLock.enablePageScroll();
    for (let i = 0; i < $fillGapTargets.length; i++) {
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-filled-gap')).toBe(null);
    }
});

test('remove fill gap target', () => {
    scrollLock.disablePageScroll();

    const $fillGapTarget = document.querySelector('#fill-gap-target');
    expect($fillGapTarget.getAttribute('data-scroll-lock-fill-gap')).toBe('');
    expect($fillGapTarget.getAttribute('data-scroll-lock-filled-gap')).toBe('true');
    scrollLock.removeFillGapTarget($fillGapTarget);
    expect($fillGapTarget.getAttribute('data-scroll-lock-fill-gap')).toBe(null);
    expect($fillGapTarget.getAttribute('data-scroll-lock-filled-gap')).toBe(null);

    const $fillGapTargets = document.querySelectorAll('.fill-gap-target');
    for (let i = 0; i < $fillGapTargets.length; i++) {
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-fill-gap')).toBe('');
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-filled-gap')).toBe('true');
    }
    scrollLock.removeFillGapTarget($fillGapTargets);
    for (let i = 0; i < $fillGapTargets.length; i++) {
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-fill-gap')).toBe(null);
        expect($fillGapTargets[i].getAttribute('data-scroll-lock-filled-gap')).toBe(null);
    }
});

test('confirm you can not add duplicate selectors', () => {
    scrollLock.disablePageScroll();
    const initialFillGapSelectors = JSON.parse(JSON.stringify(scrollLock._state.fillGapSelectors));
    scrollLock.addFillGapSelector('.duplicate-gap-selector');
    scrollLock.addFillGapSelector('.duplicate-gap-selector');
    initialFillGapSelectors.push('.duplicate-gap-selector');
    expect(scrollLock._state.fillGapSelectors).toEqual(initialFillGapSelectors);
});
