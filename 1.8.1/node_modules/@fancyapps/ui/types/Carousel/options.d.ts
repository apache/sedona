import { OptionsType as PanzoomOptionsType } from "../Panzoom/options";
import { userSlideType, Events } from "./types";
export interface PluginsOptionsType {
}
export interface classesType {
    container: string;
    viewport: string;
    track: string;
    slide: string;
    isLTR: string;
    isRTL: string;
    isHorizontal: string;
    isVertical: string;
    inTransition: string;
    isSelected: string;
}
export interface ComponentOptionsType {
    /**
     * Specify the viewport element
     */
    viewport: HTMLElement | null | ((any?: any) => HTMLElement | null);
    /**
     * Specify the track element
     */
    track: HTMLElement | null | ((any?: any) => HTMLElement | null);
    /**
     * Carousel is enabled or not; useful when combining with breakpoints
     */
    enabled: boolean;
    /**
     * Virtual slides
     */
    slides: Array<userSlideType>;
    /**
     * Horizontal (`x`) or vertical Carousel (`y`)
     */
    axis: "x" | "y" | ((any?: any) => "x" | "y");
    /**
     * The name of the transition animation when changing Carousel pages
     */
    transition: "crossfade" | "fade" | "slide" | "classic" | string | false | ((any?: any) => "crossfade" | "fade" | "slide" | "classic");
    /**
     * Number of pages to preload before/after the active page
     */
    preload: number;
    /**
     * The number of slides to group per page
     */
    slidesPerPage: number | "auto";
    /**
     * Index of initial page
     */
    initialPage: number;
    /**
     * Index of initial slide
     */
    initialSlide?: number;
    /**
     * Panzoom friction while changing page
     */
    friction: number | ((any?: any) => number);
    /**
     * If true, the Carousel will center the active page
     */
    center: boolean | ((any?: any) => boolean);
    /**
     * If true, the Carousel will scroll infinitely
     */
    infinite: boolean | ((any?: any) => boolean);
    /**
     * If true, the Carousel will fill the free space if `infinite: false`
     */
    fill: boolean | ((any?: any) => boolean);
    /**
     * If true, the Carousel will settle at any position after a swipe
     */
    dragFree: boolean | ((any?: any) => boolean);
    /**
     * If true, the Carousel will adjust its height to the height of the currently active slide(s)
     */
    adaptiveHeight: boolean | ((any?: any) => boolean);
    /**
     * Change direction of Carousel
     */
    direction: "ltr" | "rtl" | ((any?: any) => "ltr" | "rtl");
    /**
     * Custom options for the Panzoom instance
     */
    Panzoom?: Partial<PanzoomOptionsType>;
    /**
     * Class names for DOM elements
     */
    classes: Partial<classesType>;
    /**
     * Options that will be applied for the given breakpoint, overriding the base options
     */
    breakpoints?: Record<string, Omit<Partial<OptionsType>, "breakpoints">>;
    /**
     * Localization of strings
     */
    l10n: Record<string, string>;
    /**
     * Optional event listeners
     */
    on?: Partial<Events>;
}
export declare const defaultOptions: ComponentOptionsType;
export type OptionsType = PluginsOptionsType & ComponentOptionsType;
