const scrollLock = require('../dist/scroll-lock');

test('set fill gap method', () => {
    document.body.innerHTML = `
        <div id="fill-gap-target" data-scroll-lock-fill-gap></div>
    `;

    const $fillGapTarget = document.querySelector('#fill-gap-target');

    scrollLock.disablePageScroll();

    expect(scrollLock._state.fillGapMethod).toBe('padding');
    expect($fillGapTarget.getAttribute('data-scroll-lock-current-fill-gap-method')).toBe('padding');

    scrollLock.setFillGapMethod('margin');
    expect(scrollLock._state.fillGapMethod).toBe('margin');
    expect($fillGapTarget.getAttribute('data-scroll-lock-current-fill-gap-method')).toBe('margin');

    scrollLock.setFillGapMethod('width');
    expect(scrollLock._state.fillGapMethod).toBe('width');
    expect($fillGapTarget.getAttribute('data-scroll-lock-current-fill-gap-method')).toBe('width');

    scrollLock.setFillGapMethod('max-width');
    expect(scrollLock._state.fillGapMethod).toBe('max-width');
    expect($fillGapTarget.getAttribute('data-scroll-lock-current-fill-gap-method')).toBe('max-width');

    scrollLock.setFillGapMethod('none');
    expect(scrollLock._state.fillGapMethod).toBe('none');
    expect($fillGapTarget.getAttribute('data-scroll-lock-current-fill-gap-method')).toBe('none');

    const errorSpy = jest.spyOn(global.console, 'error').mockImplementation(() => {});
    scrollLock.setFillGapMethod('unsupported value');
    expect(errorSpy).toHaveBeenCalled();
    expect(scrollLock._state.fillGapMethod).toBe('none');
    expect($fillGapTarget.getAttribute('data-scroll-lock-current-fill-gap-method')).toBe('none');
});
