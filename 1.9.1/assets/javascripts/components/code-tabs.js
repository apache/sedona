import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import python from 'highlight.js/lib/languages/python';
import java from 'highlight.js/lib/languages/java';
import powershell from 'highlight.js/lib/languages/powershell';
import sql from 'highlight.js/lib/languages/sql';

// Register the languages
hljs.registerLanguage('bash', bash);
hljs.registerLanguage('python', python);
hljs.registerLanguage('java', java);
hljs.registerLanguage('powershell', powershell);
hljs.registerLanguage('sql', sql);

hljs.configure({
  ignoreUnescapedHTML: true,
});

export const codeTabs = () => {
  const initHljsSnippet = (root) => {
    const tabs = root.querySelectorAll('.hljs-snippet__tab');
    const panels = root.querySelectorAll('.hljs-snippet__panel');
    const copyBtn = root.querySelector('.hljs-snippet__copy');

    // Helper to highlight code safely
    const highlight = (codeEl) => {
      codeEl.removeAttribute('data-highlighted'); // remove previous flag
      hljs.highlightElement(codeEl);
    };

    // Highlight all panels on load
    panels.forEach((p) => highlight(p.querySelector('code')));

    // Activate a specific language
    const activate = (lang) => {
      tabs.forEach((btn) => {
        const active = btn.dataset.lang === lang;
        btn.classList.toggle('is-active', active);
        btn.setAttribute('aria-selected', String(active));
      });

      panels.forEach((panel) => {
        const show = panel.dataset.lang === lang;
        panel.hidden = !show;
        if (show) highlight(panel.querySelector('code')); // safe re-highlight
      });
    };

    // Initial tab
    const initial = root.dataset.initial || tabs[0]?.dataset.lang;
    if (initial) activate(initial);

    // Tab click handling
    tabs.forEach((btn) => {
      btn.addEventListener('click', () => activate(btn.dataset.lang));
    });

    // Copy to clipboard
    const copyCurrent = async () => {
      const current = [...panels].find((p) => !p.hidden);
      if (!current) return;

      const code = current.querySelector('code').textContent;

      const showCopied = () => {
        copyBtn.classList.add('copied');
        setTimeout(() => copyBtn.classList.remove('copied'), 1200);
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
        } catch {}
        document.body.removeChild(area);
        showCopied();
      }
    };

    if (copyBtn) {
      copyBtn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        copyCurrent();
      });
    }
  };

  // Initialize all snippet instances
  document.querySelectorAll('.hljs-snippet').forEach(initHljsSnippet);
};
