import { OptionsType } from "./options";
import { EventsType, PluginsType } from "./types";
import { States, SlideStates } from "./consts";
import { Component } from "../shared/Base/Component";
export type * from "./plugins/index";
import { Carousel } from "../Carousel/Carousel";
import { slideType, userSlideType } from "../Carousel/types";
declare module "../Carousel/types" {
    interface slideType {
        src?: string | HTMLElement;
        width?: number | "auto";
        height?: number | "auto";
        id?: string;
        display?: string;
        error?: string;
        filter?: string;
        caption?: string | HTMLElement;
        downloadSrc?: string;
        downloadFilename?: string;
        contentEl?: HTMLElement;
        captionEl?: HTMLElement;
        spinnerEl?: HTMLElement;
        closeBtnEl?: HTMLElement;
        triggerEl?: HTMLElement;
        thumbEl?: HTMLImageElement;
        thumbElSrc?: string;
        poster?: string;
        state?: SlideStates;
    }
}
export declare class Fancybox extends Component<OptionsType, EventsType> {
    static version: string;
    static defaults: Partial<OptionsType>;
    static Plugins: PluginsType;
    static openers: Map<HTMLElement, Map<string, Partial<OptionsType>>>;
    private userSlides;
    private userPlugins;
    private idle;
    private idleTimer;
    private clickTimer;
    private pwt;
    private ignoreFocusChange;
    private startedFs;
    state: States;
    id: number | string;
    container: HTMLElement | null;
    caption: HTMLElement | null;
    footer: HTMLElement | null;
    carousel: Carousel | null;
    lastFocus: HTMLElement | null;
    prevMouseMoveEvent: MouseEvent | undefined;
    get isIdle(): boolean;
    get isCompact(): boolean;
    constructor(userSlides?: Array<userSlideType>, userOptions?: Partial<OptionsType>, userPlugins?: PluginsType);
    private init;
    private initLayout;
    private initCarousel;
    private attachEvents;
    private detachEvents;
    private scale;
    private onClick;
    private onWheel;
    private onScroll;
    private onKeydown;
    private onResize;
    private onFocus;
    private onMousemove;
    private onVisibilityChange;
    private manageCloseBtn;
    private manageCaption;
    /**
     * Make sure the element, that has the focus, is inside the container
     */
    checkFocus(event?: FocusEvent): void;
    /**
     * Place focus on the first focusable element inside current slide
     */
    focus(event?: FocusEvent): void;
    /**
     * Slide carousel to the next page
     */
    next(): void;
    /**
     * Slide carousel to the previous page
     */
    prev(): void;
    /**
     * Slide carousel to page by its index with optional parameters
     */
    jumpTo(...args: any): void;
    /**
     * Check if there is another instance on top of this one
     */
    isTopmost(): boolean;
    animate(element?: HTMLElement | null, className?: string, callback?: () => void | null): void;
    stop(element: HTMLElement): void;
    /**
     * Set new content for the given slide
     */
    setContent(slide: slideType, html?: string | HTMLElement, shouldReveal?: boolean): void;
    revealContent(slide: slideType, showClass?: string | false): void;
    done(slide: slideType): void;
    /**
     * Check if the given slide is the current slide in the carousel
     */
    isCurrentSlide(slide: slideType): boolean;
    /**
     * Check if the given slide is opening slide
     */
    isOpeningSlide(slide?: slideType): boolean;
    /**
     * Show loading icon inside given slide
     */
    showLoading(slide: slideType): void;
    /**
     * Hide loading icon inside given slide
     */
    hideLoading(slide: slideType): void;
    /**
     * Show error message for given slide
     */
    setError(slide: slideType, message: string): void;
    /**
     * Clear content for given slide
     */
    clearContent(slide: slideType): void;
    /**
     * Retrieve current carousel slide
     */
    getSlide(): slideType | undefined;
    /**
     * Initiate closing
     */
    close(event?: Event, hideClass?: string | false): void;
    /**
     * Clear idle state timer
     */
    clearIdle(): void;
    /**
     * Activate idle state
     */
    setIdle(now?: boolean): void;
    /**
     * Deactivate idle state
     */
    endIdle(): void;
    /**
     * Reset idle state timer
     */
    resetIdle(): void;
    /**
     * Toggle idle state
     */
    toggleIdle(): void;
    /**
     * Toggle full-screen mode
     */
    toggleFullscreen(): void;
    /**
     * Check if the instance is being closed or already destroyed
     */
    isClosing(): boolean;
    private proceedClose;
    /**
     * Destroy the instance
     */
    destroy(): void;
    /**
     * Add a click handler that launches Fancybox after clicking on items that match the provided selector
     */
    static bind(selector: string, userOptions?: Partial<OptionsType>): void;
    /**
     * Add a click handler to the given container that launches Fancybox after clicking items that match the provided selector
     */
    static bind(container: HTMLElement | null, selector: string, userOptions?: Partial<OptionsType>): void;
    /**
     * Remove selector from the list of selectors that triggers Fancybox
     */
    static unbind(selector: string): void;
    /**
     * Remove all or one selector from the list of selectors that triggers Fancybox for the given container
     */
    static unbind(container: HTMLElement | null, selector?: string): void;
    /**
     * Immediately destroy all instances (without closing animation) and clean up
     */
    static destroy(): void;
    /**
     * Start Fancybox using click event
     */
    static fromEvent(event: MouseEvent): Fancybox;
    /**
     * Start Fancybox using the previously assigned selector
     */
    static fromSelector(selector: string, options?: Partial<OptionsType>): void;
    /**
     * Start Fancybox using the previously assigned selector for the given container
     */
    static fromSelector(container: HTMLElement | null, selector: string, options?: Partial<OptionsType>): void;
    /**
     * Start Fancybox using HTML elements
     */
    static fromNodes(nodes: Array<HTMLElement>, options?: Partial<OptionsType>): Fancybox;
    /**
     * Retrieve instance by identifier or the top most instance, if identifier is not provided
     */
    static getInstance(id?: number): Fancybox | null;
    /**
     * Retrieve reference to the current slide of the highest active Fancybox instance
     */
    static getSlide(): slideType | null;
    /**
     * Create new Fancybox instance with provided options
     */
    static show(slides?: Array<userSlideType>, options?: Partial<OptionsType>): Fancybox;
    /**
     * Slide carousel of the current instance to the next page
     */
    static next(): void;
    /**
     * Slide carousel of the current instance to the previous page
     */
    static prev(): void;
    /**
     * Close all or topmost currently active instance
     */
    static close(all?: boolean, ...args: any): void;
}
