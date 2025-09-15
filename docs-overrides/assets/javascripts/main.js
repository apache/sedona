import {industriesTabs} from './components/industries-tabs';
import {codeTabs} from './components/code-tabs';
import {typedAnimation} from './components/typed-animation';

document.addEventListener('DOMContentLoaded', () => {
  industriesTabs();
  codeTabs();
  typedAnimation();
});

document$.subscribe(function () {
  industriesTabs();
  codeTabs();
  typedAnimation();
});
