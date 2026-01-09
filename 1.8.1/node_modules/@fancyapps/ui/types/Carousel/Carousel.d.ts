import { Component } from "../shared/Base/Component";
import { Panzoom } from "../Panzoom/Panzoom";
import { PluginsType, CarouselEventsType, slideType, pageType, userSlideType } from "./types";
import { OptionsType } from "./options";
import { States } from "./consts";
export type * from "./plugins/index";
export declare class Carousel extends Component<OptionsType, CarouselEventsType> {
    static Panzoom: typeof Panzoom;
    static defaults: Partial<OptionsType>;
    static Plugins: PluginsType;
    private bp;
    private lp;
    private get axis();
    userOptions: Partial<OptionsType>;
    userPlugins: PluginsType;
    /**
     * Current state of the instance
     */
    state: States;
    /**
     * Index of the currently selected page
     */
    page: number;
    /**
     * Index of the previously selected page
     */
    prevPage: number | null;
    /**
     * Reference to the container element
     */
    container: HTMLElement;
    /**
     * Reference to the viewport element
     */
    viewport: HTMLElement | null;
    /**
     * Reference to the track element
     */
    track: HTMLElement | null;
    /**
     * An array containing objects associated with the carousel slides
     */
    slides: Array<slideType>;
    /**
     * An array containing objects associated with the carousel pages
     */
    pages: Array<pageType>;
    /**
     * Reference to the Panzoom instance
     */
    panzoom: Panzoom | null;
    /**
     * A collection of indexes for slides that are currently being animated using CSS animation
     */
    inTransition: Set<number>;
    /**
     * Total slide width or slide height for a vertical carousel
     */
    contentDim: number;
    /**
     * Viewport width for a horizontal or height for a vertical carousel
     */
    viewportDim: number;
    /**
     * True if the carousel is currently enabled
     */
    get isEnabled(): boolean;
    /**
     * True if the carousel can navigate infinitely
     */
    get isInfinite(): boolean;
    /**
     * True if the carousel is in RTL mode
     */
    get isRTL(): boolean;
    /**
     * True if the carousel is horizontal
     */
    get isHorizontal(): boolean;
    constructor(element: HTMLElement | string | null, userOptions?: Partial<OptionsType>, userPlugins?: PluginsType);
    private processOptions;
    private init;
    private initLayout;
    private initSlides;
    private setInitialPage;
    private setInitialPosition;
    private initPanzoom;
    private attachEvents;
    private createPages;
    private processPages;
    private getPageFromIndex;
    private getSlideMetrics;
    private getBounds;
    private repositionSlides;
    private createSlideEl;
    private removeSlideEl;
    private transitionTo;
    private manageSlideVisiblity;
    private markSelectedSlides;
    private flipInfiniteTrack;
    private lazyLoadImg;
    private lazyLoadSlide;
    private onAnimationEnd;
    private onDecel;
    private onClick;
    private onSlideTo;
    private onChange;
    private onRefresh;
    private onScroll;
    private onResize;
    private onBeforeTransform;
    private onEndAnimation;
    /**
     * Reset carousel after initializing it, this method allows to replace options and plugins
     */
    reInit(userOptions?: Partial<OptionsType> | null, userPlugins?: PluginsType | null): void;
    /**
     * Slide to page by its index with optional parameters
     */
    slideTo(pageIndex?: number | string, { friction, transition, }?: {
        friction?: number;
        transition?: string | false;
    }): void;
    /**
     * Re-center carousel
     */
    slideToClosest(opts: any): void;
    /**
     * Slide to the next page
     */
    slideNext(): void;
    /**
     * Slide to the previous page
     */
    slidePrev(): void;
    /**
     * Stop transition effect
     */
    clearTransitions(): void;
    /**
     * Create the slide(s) and add by the specified index
     */
    addSlide(index: number, what: userSlideType | Array<userSlideType>): void;
    /**
     * Create slide(s) and prepend to the beginning of the carousel
     */
    prependSlide(what: userSlideType | Array<userSlideType>): void;
    /**
     * Create slide(s) and add to the end of the carousel
     */
    appendSlide(what: userSlideType | Array<userSlideType>): void;
    /**
     * Remove slide and DOM elements from the carousel
     */
    removeSlide(index: number): void;
    /**
     * Forces to recalculate elements metrics
     */
    updateMetrics(): void;
    /**
     * Get the progress of the active or selected page relative to the "center"
     */
    getProgress(index?: number, raw?: boolean, ignoreInfinite?: boolean): number;
    /**
     * Set the height of the viewport to match the maximum height of the slides on the current page
     */
    setViewportHeight(): void;
    /**
     * Return the index of the page containing the slide
     */
    getPageForSlide(index: number): number;
    /**
     * Return all slides that are at least partially visible
     */
    getVisibleSlides(multiplier?: number): Set<slideType>;
    /**
     * Get the page index for a given Panzoom position
     */
    getPageFromPosition(pos?: number): {
        pageIndex: number;
        page: number;
    };
    /**
     * Manually update page index based on the Panzoom position
     */
    setPageFromPosition(): void;
    /**
     * Destroy the instance
     */
    destroy(): void;
}
