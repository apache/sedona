const scrollLock = require('../dist/scroll-lock');

test('add lockable selector', () => {
    const initialLockableSelectors = JSON.parse(JSON.stringify(scrollLock._state.lockableSelectors));
    scrollLock.addLockableSelector('.lockable-selector');
    initialLockableSelectors.push('.lockable-selector');
    expect(scrollLock._state.lockableSelectors).toEqual(initialLockableSelectors);

    scrollLock.addLockableSelector(['.lockable-selector-1', '.lockable-selector-2']);
    initialLockableSelectors.push('.lockable-selector-1');
    initialLockableSelectors.push('.lockable-selector-2');
    expect(scrollLock._state.lockableSelectors).toEqual(initialLockableSelectors);
});

test('add lockable target', () => {
    document.body.innerHTML = `
		<div id="lockable-target"></div>
		
		<div class="lockable-target"></div>
		<div class="lockable-target"></div>
		<div class="lockable-target"></div>
	`;

    const $lockableTarget = document.querySelector('#lockable-target');
    expect($lockableTarget.getAttribute('data-scroll-lock-lockable')).toBe(null);
    scrollLock.addLockableTarget($lockableTarget);
    expect($lockableTarget.getAttribute('data-scroll-lock-lockable')).toBe('');

    const $lockableTargets = document.querySelectorAll('.lockable-target');
    for (let i = 0; i < $lockableTargets.length; i++) {
        expect($lockableTargets[i].getAttribute('data-scroll-lock-lockable')).toBe(null);
    }
    scrollLock.addLockableTarget($lockableTargets);
    for (let i = 0; i < $lockableTargets.length; i++) {
        expect($lockableTargets[i].getAttribute('data-scroll-lock-lockable')).toBe('');
    }
});
