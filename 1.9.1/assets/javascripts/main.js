import {industriesTabs} from './components/industries-tabs';
import {codeTabs} from './components/code-tabs';
import {typedAnimation} from './components/typed-animation';
import {searchTags} from './components/search-tags';

document.addEventListener('DOMContentLoaded', () => {
  industriesTabs();
  codeTabs();
  typedAnimation();
  searchTags();
});

document$.subscribe(function () {
  industriesTabs();
  codeTabs();
  typedAnimation();
});
