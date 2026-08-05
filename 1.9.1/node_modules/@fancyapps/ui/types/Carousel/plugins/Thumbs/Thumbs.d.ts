import { Plugin } from "../../../shared/Base/Plugin";
import { OptionsType as PanzoomOptionsType } from "../../../Panzoom/options";
import type { Carousel } from "../../Carousel";
import { OptionsType as CarouselOptionsType } from "../../options";
type ThumbsEventsType = "ready" | "createSlide" | "disabled";
type ThumbsEvents = Record<ThumbsEventsType, (...args: any[]) => void>;
export type ThumbsOptionsType = {
    /**
     * Customize carousel options
     */
    Carousel?: Partial<CarouselOptionsType>;
    /**
     * Customize panzoom options
     */
    Panzoom?: Partial<PanzoomOptionsType>;
    /**
     * Class names for DOM elements
     */
    classes: {
        container?: string;
        viewport?: string;
        track?: string;
        slide?: string;
        isResting?: string;
        isSelected?: string;
        isLoading?: string;
        hasThumbs?: string;
    };
    /**
     * Minimum number of slides with thumbnails in the carousel to create Thumbs
     */
    minCount: number;
    /**
     * Optional event listeners
     */
    on?: Partial<ThumbsEvents>;
    /**
     * Change where thumbnail container is appended
     */
    parentEl?: HTMLElement | null | (() => HTMLElement | null);
    /**
     * Template for the thumbnail element
     */
    thumbTpl: string;
    /**
     * Choose a type - "classic" (syncs two instances of the carousel) or "modern" (Apple Photos style)
     */
    type: "classic" | "modern";
};
export declare const defaultOptions: ThumbsOptionsType;
declare module "../../../Carousel/options" {
    interface PluginsOptionsType {
        Thumbs: Boolean | Partial<ThumbsOptionsType>;
    }
}
declare module "../../../Carousel/types" {
    interface slideType {
        thumbSrc?: string;
        thumbClipWidth?: number;
        thumbWidth?: number;
        thumbHeight?: number;
        thumbSlideEl?: HTMLElement;
    }
}
export declare enum States {
    Init = 0,
    Ready = 1,
    Hidden = 2
}
export declare class Thumbs extends Plugin<Carousel, ThumbsOptionsType, ThumbsEventsType> {
    static defaults: ThumbsOptionsType;
    type: "modern" | "classic";
    get isModern(): boolean;
    container: HTMLElement | null;
    track: HTMLElement | null;
    private carousel;
    private thumbWidth;
    private thumbClipWidth;
    private thumbHeight;
    private thumbGap;
    private thumbExtraGap;
    state: States;
    private onInitSlide;
    private onInitSlides;
    private onChange;
    private onRefresh;
    isDisabled(): Boolean;
    private getThumb;
    private addSlide;
    private getSlides;
    private resizeModernSlide;
    private updateProps;
    build(): void;
    private onClick;
    private getShift;
    private setProps;
    private shiftModern;
    private cleanup;
    attach(): void;
    detach(): void;
}
export {};
