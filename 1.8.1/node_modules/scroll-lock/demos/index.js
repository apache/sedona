var eachNode = function(nodeList, callback) {
    if (nodeList && callback) {
        for (let i = 0; i < nodeList.length; i++) {
            if (callback(nodeList[i], i, nodeList.length) === true) {
                break;
            }
        }
    }
};

var $disableScrollEls = document.querySelectorAll('[data-demo-disable-scroll]');
var $enableScrollEls = document.querySelectorAll('[data-demo-enable-scroll]');
var $stateEnableEls = document.querySelectorAll('[data-demo-state-enable]');
var $stateDisableEls = document.querySelectorAll('[data-demo-state-disable]');
var $fillGapMethodSelect = document.querySelector('[data-demo-fill-gap-method-select]');

var disableStates = function() {
    eachNode($stateEnableEls, function($el) {
        $el.style.display = 'none';
    });
    eachNode($stateDisableEls, function($el) {
        $el.style.display = 'block';
    });
};
var enableStates = function() {
    eachNode($stateEnableEls, function($el) {
        $el.style.display = 'block';
    });
    eachNode($stateDisableEls, function($el) {
        $el.style.display = 'none';
    });
};

var disableScroll = function() {
    if (scrollLock.getScrollState()) {
        scrollLock.disablePageScroll();
        disableStates();
    }
};
var enableScroll = function() {
    if (!scrollLock.getScrollState()) {
        scrollLock.enablePageScroll();
        enableStates();
    }
};

eachNode($disableScrollEls, function($el) {
    $el.addEventListener('click', disableScroll);
});
eachNode($enableScrollEls, function($el) {
    $el.addEventListener('click', enableScroll);
});

$fillGapMethodSelect.addEventListener('change', function(e) {
    var value = e.target.value;
    scrollLock.setFillGapMethod(value);
});
