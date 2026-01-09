const scrollLock = require('../dist/scroll-lock');

test('deprecated methods', () => {
    expect(scrollLock.hide).toBeInstanceOf(Function);
    expect(scrollLock.show).toBeInstanceOf(Function);
    expect(scrollLock.toggle).toBeInstanceOf(Function);
    expect(scrollLock.getState).toBeInstanceOf(Function);
    expect(scrollLock.getWidth).toBeInstanceOf(Function);
    expect(scrollLock.getCurrentWidth).toBeInstanceOf(Function);
    expect(scrollLock.setScrollableTargets).toBeInstanceOf(Function);
    expect(scrollLock.setFillGapSelectors).toBeInstanceOf(Function);
    expect(scrollLock.setFillGapTargets).toBeInstanceOf(Function);
    expect(scrollLock.clearQueue).toBeInstanceOf(Function);
});
