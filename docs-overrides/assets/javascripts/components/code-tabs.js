import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import python from 'highlight.js/lib/languages/python';
import java from 'highlight.js/lib/languages/java';
import powershell from 'highlight.js/lib/languages/powershell';
import sql from 'highlight.js/lib/languages/sql';

// Register the languages you need
hljs.registerLanguage('python', python);
hljs.registerLanguage('java', java);
hljs.registerLanguage('powershell', powershell);
hljs.registerLanguage('bash', bash);
hljs.registerLanguage('sql', sql);

export const codeTabs = () => {

  // Initialize every snippet block independently
  const initHljsSnippet = (root) => {
    const tabs = root.querySelectorAll('.hljs-snippet__tab');
    const panels = root.querySelectorAll('.hljs-snippet__panel');
    const copyBtn = root.querySelector('.hljs-snippet__copy');

    // Highlight all panels once on load
    panels.forEach((p) => hljs.highlightElement(p.querySelector('code')));

    const activate = (lang) => {
      tabs.forEach((btn) => {
        const active = btn.dataset.lang === lang;
        btn.classList.toggle('is-active', active);
        btn.setAttribute('aria-selected', String(active));
      });

      panels.forEach((panel) => {
        panel.hidden = panel.dataset.lang !== lang;
      });
    };


    // Initial tab
    const initial = root.dataset.initial || tabs[0]?.dataset.lang;
    if (initial) activate(initial);

    // Tab click handling
    tabs.forEach((btn) => {
      btn.addEventListener('click', () => activate(btn.dataset.lang));
    });

    // Copy to clipboard (with fallback)
    const copyCurrent = async () => {
      const current = [...panels].find((p) => !p.hidden);
      if (!current) return;

      const code = current.querySelector('code').textContent;

      const showCopied = () => {
        copyBtn.classList.add('copied');            // apply CSS class
        setTimeout(() => copyBtn.classList.remove('copied'), 1200); // remove class
      };

      try {
        await navigator.clipboard.writeText(code);
        showCopied();
      } catch {
        const area = document.createElement('textarea');
        area.value = code;
        area.style.position = 'fixed';
        area.style.opacity = '0';
        document.body.appendChild(area);
        area.select();
        try {
          document.execCommand('copy');
        } catch {
        }
        document.body.removeChild(area);
        showCopied();
      }
    };

    if (copyBtn) {
      copyBtn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();

        copyCurrent();

      });
    }


  };

  // Boot all snippet instances on the page
  document.querySelectorAll('.hljs-snippet').forEach(initHljsSnippet);

};
