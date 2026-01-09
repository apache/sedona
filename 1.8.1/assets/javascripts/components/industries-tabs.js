'use strict';

export const industriesTabs = () => {
  // Loop through all tab components on the page
  document.querySelectorAll('.industries-tabs').forEach((tabs) => {
    const tabHeads = tabs.querySelectorAll('.industries-tabs__head-item');
    const tabBodies = tabs.querySelectorAll('.industries-tabs__body-item');

    tabHeads.forEach((head, index) => {
      head.addEventListener('click', () => {
        // Remove active class from all
        tabHeads.forEach((h) => h.classList.remove('active'));
        tabBodies.forEach((b) => b.classList.remove('active'));

        // Add active class to clicked head & corresponding body
        head.classList.add('active');
        tabBodies[index].classList.add('active');
      });
    });
  });
};
