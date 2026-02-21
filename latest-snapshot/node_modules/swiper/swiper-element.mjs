/**
 * Swiper Custom Element 12.1.2
 * Most modern mobile touch slider and framework with hardware accelerated transitions
 * https://swiperjs.com
 *
 * Copyright 2014-2026 Vladimir Kharlampidi
 *
 * Released under the MIT License
 *
 * Released on: February 18, 2026
 */

import { S as Swiper } from './shared/swiper-core.mjs';
import { p as paramsList, n as needsNavigation, a as needsPagination, b as needsScrollbar, u as updateSwiper, c as attrToProp } from './shared/update-swiper.mjs';
import { g as getParams } from './shared/get-element-params.mjs';
import { s as setInnerHTML } from './shared/utils.mjs';

/* eslint-disable spaced-comment */

const SwiperCSS = `:host{--swiper-theme-color:#007aff}:host{display:block;margin-left:auto;margin-right:auto;position:relative;z-index:1}.swiper{display:block;height:100%;list-style:none;margin-left:auto;margin-right:auto;overflow:hidden;padding:0;position:relative;width:100%;z-index:1}.swiper-vertical>.swiper-wrapper{flex-direction:column}.swiper-wrapper{box-sizing:initial;display:flex;height:100%;position:relative;transition-property:transform;transition-timing-function:var(--swiper-wrapper-transition-timing-function,initial);width:100%;z-index:1}.swiper-android ::slotted(swiper-slide),.swiper-ios ::slotted(swiper-slide),.swiper-wrapper{transform:translateZ(0)}.swiper-horizontal{touch-action:pan-y}.swiper-vertical{touch-action:pan-x}::slotted(swiper-slide){display:block;flex-shrink:0;height:100%;position:relative;transition-property:transform;width:100%}::slotted(.swiper-slide-invisible-blank){visibility:hidden}.swiper-autoheight,.swiper-autoheight ::slotted(swiper-slide){height:auto}.swiper-autoheight .swiper-wrapper{align-items:flex-start;transition-property:transform,height}.swiper-backface-hidden ::slotted(swiper-slide){backface-visibility:hidden;transform:translateZ(0)}.swiper-3d.swiper-css-mode .swiper-wrapper{perspective:1200px}.swiper-3d .swiper-wrapper{transform-style:preserve-3d}.swiper-3d{perspective:1200px}.swiper-3d .swiper-cube-shadow,.swiper-3d ::slotted(swiper-slide){transform-style:preserve-3d}.swiper-css-mode>.swiper-wrapper{overflow:auto;scrollbar-width:none;-ms-overflow-style:none}.swiper-css-mode>.swiper-wrapper::-webkit-scrollbar{display:none}.swiper-css-mode ::slotted(swiper-slide){scroll-snap-align:start start}.swiper-css-mode.swiper-horizontal>.swiper-wrapper{scroll-snap-type:x mandatory}.swiper-css-mode.swiper-horizontal ::slotted(swiper-slide):first-child{margin-inline-start:var(--swiper-slides-offset-before);scroll-margin-inline-start:var(--swiper-slides-offset-before)}.swiper-css-mode.swiper-horizontal ::slotted(swiper-slide):last-child{margin-inline-end:var(--swiper-slides-offset-after)}.swiper-css-mode.swiper-vertical>.swiper-wrapper{scroll-snap-type:y mandatory}.swiper-css-mode.swiper-vertical ::slotted(swiper-slide):first-child{margin-block-start:var(--swiper-slides-offset-before);scroll-margin-block-start:var(--swiper-slides-offset-before)}.swiper-css-mode.swiper-vertical ::slotted(swiper-slide):last-child{margin-block-end:var(--swiper-slides-offset-after)}.swiper-css-mode.swiper-free-mode>.swiper-wrapper{scroll-snap-type:none}.swiper-css-mode.swiper-free-mode ::slotted(swiper-slide){scroll-snap-align:none}.swiper-css-mode.swiper-centered>.swiper-wrapper:before{content:"";flex-shrink:0;order:9999}.swiper-css-mode.swiper-centered ::slotted(swiper-slide){scroll-snap-align:center center;scroll-snap-stop:always}.swiper-css-mode.swiper-centered.swiper-horizontal ::slotted(swiper-slide):first-child{margin-inline-start:var(--swiper-centered-offset-before)}.swiper-css-mode.swiper-centered.swiper-horizontal>.swiper-wrapper:before{height:100%;min-height:1px;width:var(--swiper-centered-offset-after)}.swiper-css-mode.swiper-centered.swiper-vertical ::slotted(swiper-slide):first-child{margin-block-start:var(--swiper-centered-offset-before)}.swiper-css-mode.swiper-centered.swiper-vertical>.swiper-wrapper:before{height:var(--swiper-centered-offset-after);min-width:1px;width:100%}`
const SwiperSlideCSS = `::slotted(.swiper-slide-shadow),::slotted(.swiper-slide-shadow-bottom),::slotted(.swiper-slide-shadow-left),::slotted(.swiper-slide-shadow-right),::slotted(.swiper-slide-shadow-top){height:100%;left:0;pointer-events:none;position:absolute;top:0;width:100%;z-index:10}::slotted(.swiper-slide-shadow){background:#00000026}::slotted(.swiper-slide-shadow-left){background-image:linear-gradient(270deg,#00000080,#0000)}::slotted(.swiper-slide-shadow-right){background-image:linear-gradient(90deg,#00000080,#0000)}::slotted(.swiper-slide-shadow-top){background-image:linear-gradient(0deg,#00000080,#0000)}::slotted(.swiper-slide-shadow-bottom){background-image:linear-gradient(180deg,#00000080,#0000)}.swiper-lazy-preloader{animation:swiper-preloader-spin 1s linear infinite;border:4px solid var(--swiper-preloader-color,var(--swiper-theme-color));border-radius:50%;border-top:4px solid #0000;box-sizing:border-box;height:42px;left:50%;margin-left:-21px;margin-top:-21px;position:absolute;top:50%;transform-origin:50%;width:42px;z-index:10}@keyframes swiper-preloader-spin{0%{transform:rotate(0deg)}to{transform:rotate(1turn)}}::slotted(.swiper-slide-shadow-cube.swiper-slide-shadow-bottom),::slotted(.swiper-slide-shadow-cube.swiper-slide-shadow-left),::slotted(.swiper-slide-shadow-cube.swiper-slide-shadow-right),::slotted(.swiper-slide-shadow-cube.swiper-slide-shadow-top){backface-visibility:hidden;z-index:0}::slotted(.swiper-slide-shadow-flip.swiper-slide-shadow-bottom),::slotted(.swiper-slide-shadow-flip.swiper-slide-shadow-left),::slotted(.swiper-slide-shadow-flip.swiper-slide-shadow-right),::slotted(.swiper-slide-shadow-flip.swiper-slide-shadow-top){backface-visibility:hidden;z-index:0}::slotted(.swiper-zoom-container){align-items:center;display:flex;height:100%;justify-content:center;text-align:center;width:100%}::slotted(.swiper-zoom-container)>canvas,::slotted(.swiper-zoom-container)>img,::slotted(.swiper-zoom-container)>svg{max-height:100%;max-width:100%;object-fit:contain}`

class DummyHTMLElement {}
const ClassToExtend = typeof window === 'undefined' || typeof HTMLElement === 'undefined' ? DummyHTMLElement : HTMLElement;
const addStyle = (shadowRoot, styles) => {
  if (typeof CSSStyleSheet !== 'undefined' && shadowRoot.adoptedStyleSheets) {
    const styleSheet = new CSSStyleSheet();
    styleSheet.replaceSync(styles);
    shadowRoot.adoptedStyleSheets = [styleSheet];
  } else {
    const style = document.createElement('style');
    style.rel = 'stylesheet';
    style.textContent = styles;
    shadowRoot.appendChild(style);
  }
};
class SwiperContainer extends ClassToExtend {
  constructor() {
    super();
    this.attachShadow({
      mode: 'open'
    });
  }
  cssStyles() {
    return [SwiperCSS,
    // eslint-disable-line
    ...(this.injectStyles && Array.isArray(this.injectStyles) ? this.injectStyles : [])].join('\n');
  }
  cssLinks() {
    return this.injectStylesUrls || [];
  }
  calcSlideSlots() {
    const currentSideSlots = this.slideSlots || 0;
    // slide slots
    const slideSlotChildren = [...this.querySelectorAll(`[slot^=slide-]`)].map(child => {
      return parseInt(child.getAttribute('slot').split('slide-')[1], 10);
    });
    this.slideSlots = slideSlotChildren.length ? Math.max(...slideSlotChildren) + 1 : 0;
    if (!this.rendered) return;
    if (this.slideSlots > currentSideSlots) {
      for (let i = currentSideSlots; i < this.slideSlots; i += 1) {
        const slideEl = document.createElement('swiper-slide');
        slideEl.setAttribute('part', `slide slide-${i + 1}`);
        const slotEl = document.createElement('slot');
        slotEl.setAttribute('name', `slide-${i + 1}`);
        slideEl.appendChild(slotEl);
        this.shadowRoot.querySelector('.swiper-wrapper').appendChild(slideEl);
      }
    } else if (this.slideSlots < currentSideSlots) {
      const slides = this.swiper.slides;
      for (let i = slides.length - 1; i >= 0; i -= 1) {
        if (i > this.slideSlots) {
          slides[i].remove();
        }
      }
    }
  }
  render() {
    if (this.rendered) return;
    this.calcSlideSlots();

    // local styles
    let localStyles = this.cssStyles();
    if (this.slideSlots > 0) {
      localStyles = localStyles.replace(/::slotted\(([a-z-0-9.]*)\)/g, '$1');
    }
    if (localStyles.length) {
      addStyle(this.shadowRoot, localStyles);
    }
    this.cssLinks().forEach(url => {
      const linkExists = this.shadowRoot.querySelector(`link[href="${url}"]`);
      if (linkExists) return;
      const linkEl = document.createElement('link');
      linkEl.rel = 'stylesheet';
      linkEl.href = url;
      this.shadowRoot.appendChild(linkEl);
    });
    // prettier-ignore
    const el = document.createElement('div');
    el.classList.add('swiper');
    el.part = 'container';

    // prettier-ignore
    setInnerHTML(el, `
      <slot name="container-start"></slot>
      <div class="swiper-wrapper" part="wrapper">
        <slot></slot>
        ${Array.from({
      length: this.slideSlots
    }).map((_, index) => `
        <swiper-slide part="slide slide-${index}">
          <slot name="slide-${index}"></slot>
        </swiper-slide>
        `).join('')}
      </div>
      <slot name="container-end"></slot>
      ${needsNavigation(this.passedParams) ? `
        <div part="button-prev" class="swiper-button-prev"></div>
        <div part="button-next" class="swiper-button-next"></div>
      ` : ''}
      ${needsPagination(this.passedParams) ? `
        <div part="pagination" class="swiper-pagination"></div>
      ` : ''}
      ${needsScrollbar(this.passedParams) ? `
        <div part="scrollbar" class="swiper-scrollbar"></div>
      ` : ''}
    `);
    this.shadowRoot.appendChild(el);
    this.rendered = true;
  }
  initialize() {
    if (this.swiper && this.swiper.initialized) return;
    const {
      params: swiperParams,
      passedParams
    } = getParams(this);
    this.swiperParams = swiperParams;
    this.passedParams = passedParams;
    delete this.swiperParams.init;
    this.render();

    // eslint-disable-next-line
    this.swiper = new Swiper(this.shadowRoot.querySelector('.swiper'), {
      ...(swiperParams.virtual ? {} : {
        observer: true
      }),
      ...swiperParams,
      touchEventsTarget: 'container',
      onAny: (name, ...args) => {
        if (name === 'observerUpdate') {
          this.calcSlideSlots();
        }
        const eventName = swiperParams.eventsPrefix ? `${swiperParams.eventsPrefix}${name.toLowerCase()}` : name.toLowerCase();
        const event = new CustomEvent(eventName, {
          detail: args,
          bubbles: name !== 'hashChange',
          cancelable: true
        });
        this.dispatchEvent(event);
      }
    });
  }
  connectedCallback() {
    if (this.swiper && this.swiper.initialized && this.nested && this.closest('swiper-slide') && this.closest('swiper-slide').swiperLoopMoveDOM) {
      return;
    }
    if (this.init === false || this.getAttribute('init') === 'false') {
      return;
    }
    this.initialize();
  }
  disconnectedCallback() {
    if (this.nested && this.closest('swiper-slide') && this.closest('swiper-slide').swiperLoopMoveDOM) {
      return;
    }
    if (this.swiper && this.swiper.destroy) {
      this.swiper.destroy();
    }
  }
  updateSwiperOnPropChange(propName, propValue) {
    const {
      params: swiperParams,
      passedParams
    } = getParams(this, propName, propValue);
    this.passedParams = passedParams;
    this.swiperParams = swiperParams;
    if (this.swiper && this.swiper.params[propName] === propValue) {
      return;
    }
    updateSwiper({
      swiper: this.swiper,
      passedParams: this.passedParams,
      changedParams: [attrToProp(propName)],
      ...(propName === 'navigation' && passedParams[propName] ? {
        prevEl: '.swiper-button-prev',
        nextEl: '.swiper-button-next'
      } : {}),
      ...(propName === 'pagination' && passedParams[propName] ? {
        paginationEl: '.swiper-pagination'
      } : {}),
      ...(propName === 'scrollbar' && passedParams[propName] ? {
        scrollbarEl: '.swiper-scrollbar'
      } : {})
    });
  }
  attributeChangedCallback(attr, prevValue, newValue) {
    if (!(this.swiper && this.swiper.initialized)) return;
    if (prevValue === 'true' && newValue === null) {
      newValue = false;
    }
    this.updateSwiperOnPropChange(attr, newValue);
  }
  static get observedAttributes() {
    const attrs = paramsList.filter(param => param.includes('_')).map(param => param.replace(/[A-Z]/g, v => `-${v}`).replace('_', '').toLowerCase());
    return attrs;
  }
}
paramsList.forEach(paramName => {
  if (paramName === 'init') return;
  paramName = paramName.replace('_', '');
  Object.defineProperty(SwiperContainer.prototype, paramName, {
    configurable: true,
    get() {
      return (this.passedParams || {})[paramName];
    },
    set(value) {
      if (!this.passedParams) this.passedParams = {};
      this.passedParams[paramName] = value;
      if (!(this.swiper && this.swiper.initialized)) return;
      this.updateSwiperOnPropChange(paramName, value);
    }
  });
});
class SwiperSlide extends ClassToExtend {
  constructor() {
    super();
    this.attachShadow({
      mode: 'open'
    });
  }
  render() {
    const lazy = this.lazy || this.getAttribute('lazy') === '' || this.getAttribute('lazy') === 'true';
    addStyle(this.shadowRoot, SwiperSlideCSS);
    this.shadowRoot.appendChild(document.createElement('slot'));
    if (lazy) {
      const lazyDiv = document.createElement('div');
      lazyDiv.classList.add('swiper-lazy-preloader');
      lazyDiv.part.add('preloader');
      this.shadowRoot.appendChild(lazyDiv);
    }
  }
  initialize() {
    this.render();
  }
  connectedCallback() {
    if (this.swiperLoopMoveDOM) {
      return;
    }
    this.initialize();
  }
}

// eslint-disable-next-line
const register = () => {
  if (typeof window === 'undefined') return;
  if (!window.customElements.get('swiper-container')) window.customElements.define('swiper-container', SwiperContainer);
  if (!window.customElements.get('swiper-slide')) window.customElements.define('swiper-slide', SwiperSlide);
};
if (typeof window !== 'undefined') {
  window.SwiperElementRegisterParams = params => {
    paramsList.push(...params);
  };
}

export { SwiperContainer, SwiperSlide, register };
