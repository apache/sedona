const PLATFORM_RULES = [
  {pattern: /\/api\/flink\//, label: 'SedonaFlink', cls: 'flink'},
  {pattern: /\/api\/snowflake\//, label: 'SedonaSnow', cls: 'snow'},
  {pattern: /\/setup\/flink\//, label: 'SedonaFlink', cls: 'flink'},
  {pattern: /\/setup\/snowflake\//, label: 'SedonaSnow', cls: 'snow'},
  {pattern: /\/tutorial\/flink\//, label: 'SedonaFlink', cls: 'flink'},
  {pattern: /\/tutorial\/snowflake\//, label: 'SedonaSnow', cls: 'snow'},
  {pattern: /sedonaflink\/?$/, label: 'SedonaFlink', cls: 'flink'},
  {pattern: /sedonasnow\/?$/, label: 'SedonaSnow', cls: 'snow'},
  {pattern: /\/api\/sql\//, label: 'SedonaSpark', cls: 'spark'},
  {pattern: /\/api\/stats\//, label: 'SedonaSpark', cls: 'spark'},
  {pattern: /\/api\/viz\//, label: 'SedonaSpark', cls: 'spark'},
  {pattern: /\/setup\/(?!flink|snowflake)/, label: 'SedonaSpark', cls: 'spark'},
  {
    pattern: /\/tutorial\/(?!flink|snowflake)/,
    label: 'SedonaSpark',
    cls: 'spark',
  },
  {pattern: /sedonaspark\/?$/, label: 'SedonaSpark', cls: 'spark'},
];

function getPlatform(href) {
  for (const rule of PLATFORM_RULES) {
    if (rule.pattern.test(href)) {
      return rule;
    }
  }
  return null;
}

function tagResult(item) {
  if (item.querySelector('.search-tag')) return;

  const link = item.querySelector('a');
  if (!link) return;

  const platform = getPlatform(link.getAttribute('href') || '');
  if (!platform) return;

  const tag = document.createElement('span');
  tag.className = `search-tag search-tag--${platform.cls}`;
  tag.textContent = platform.label;

  const title = link.querySelector('h1, h2');
  if (title) {
    title.appendChild(tag);
  }
}

function handleMutations(mutationsList) {
  for (const mutation of mutationsList) {
    for (const node of mutation.addedNodes) {
      if (!(node instanceof HTMLElement)) continue;

      if (node.matches('.md-search-result__item')) {
        tagResult(node);
        continue;
      }

      const items = node.querySelectorAll
        ? node.querySelectorAll('.md-search-result__item')
        : [];
      items.forEach(tagResult);
    }
  }
}

export const searchTags = () => {
  const resultList = document.querySelector('.md-search-result__list');
  if (!resultList) return;

  resultList.querySelectorAll('.md-search-result__item').forEach(tagResult);

  const observer = new MutationObserver(handleMutations);
  observer.observe(resultList, {childList: true, subtree: true});
};
