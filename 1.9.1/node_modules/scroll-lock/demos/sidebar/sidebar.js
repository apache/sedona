var eachNode = function(nodeList, callback) {
    if (nodeList && callback) {
        for (let i = 0; i < nodeList.length; i++) {
            if (callback(nodeList[i], i, nodeList.length) === true) {
                break;
            }
        }
    }
};

var $modal = document.querySelector('.js-modal');
var $modalScroll = document.querySelector('.js-modal-scroll');
var $modalBackdoor = document.querySelector('.js-modal-backdoor');
var $openModalButtons = document.querySelectorAll('.js-open-modal');
var $closeModalButtons = document.querySelectorAll('.js-close-modal');

var $sidebarScroll = document.querySelector('.js-sidebar-scroll');
scrollLock.addLockableTarget($sidebarScroll);

var $navbarFillGap = document.querySelectorAll('.js-navbar-fill-gap');
scrollLock.addFillGapTarget($navbarFillGap);

function openModal() {
    $modalBackdoor.style.display = 'block';
    $modal.style.display = 'block';
    scrollLock.disablePageScroll($modalScroll);
}

function closeModal() {
    $modalBackdoor.style.display = '';
    $modal.style.display = '';
    scrollLock.enablePageScroll($modalScroll);
}

eachNode($openModalButtons, function($el) {
    $el.addEventListener('click', openModal);
});

eachNode($closeModalButtons, function($el) {
    $el.addEventListener('click', closeModal);
});
