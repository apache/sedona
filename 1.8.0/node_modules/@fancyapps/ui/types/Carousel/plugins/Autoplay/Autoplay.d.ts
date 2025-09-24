import { Plugin } from "../../../shared/Base/Plugin";
import { Carousel } from "../../Carousel";
type AutoplayEventsType = "start" | "set" | "pause" | "resume" | "stop";
type AutoplayEvents = Record<AutoplayEventsType, (...args: any[]) => void>;
export type AutoplayOptionsType = {
    /**
     * If autoplay should start automatically after initialization
     */
    autoStart: boolean;
    /**
     * Optional event listeners
     */
    on?: Partial<AutoplayEvents>;
    /**
     * If autoplay should pause when the user hovers over the container
     */
    pauseOnHover: boolean;
    /**
     * Change where progress bar is appended
     */
    progressParentEl: HTMLElement | null | ((any: any) => HTMLElement | null);
    /**
     * If element should be created to display the autoplay progress
     */
    showProgress: boolean;
    /**
     * Delay (in milliseconds) before the slide change
     */
    timeout: number;
};
declare module "../../../Carousel/options" {
    interface PluginsOptionsType {
        Autoplay: Boolean | Partial<AutoplayOptionsType>;
    }
}
export declare class Autoplay extends Plugin<Carousel, AutoplayOptionsType, AutoplayEventsType> {
    static defaults: AutoplayOptionsType;
    private state;
    private inHover;
    private timer;
    private progressBar;
    get isActive(): boolean;
    private onReady;
    private onChange;
    private onSettle;
    private onVisibilityChange;
    private onMouseEnter;
    private onMouseLeave;
    private onTimerEnd;
    private removeProgressBar;
    private createProgressBar;
    private set;
    private clear;
    start(): void;
    stop(): void;
    pause(): void;
    resume(): void;
    toggle(): void;
    attach(): void;
    detach(): void;
}
export {};
