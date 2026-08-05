import { Plugin } from "../../../shared/Base/Plugin";
import { Carousel } from "../../Carousel";
export type SyncOptionsType = {
    /**
     * Target carousel sliding animation friction (after clicking navigation carousel slide)
     */
    friction: number;
    /**
     * An instance of a carousel acting as navigation
     */
    nav?: Carousel;
    /**
     * An instance of a carousel acting as target
     */
    target?: Carousel;
};
declare module "../../../Carousel/options" {
    interface PluginsOptionsType {
        /**
         * Sync instance to another and make it act as navigation
         */
        Sync?: Boolean | Partial<SyncOptionsType>;
    }
}
type SyncEventsType = "";
export declare class Sync extends Plugin<Carousel, SyncOptionsType, SyncEventsType> {
    static defaults: SyncOptionsType;
    private selectedIndex;
    private target;
    private nav;
    private addAsTargetFor;
    private addAsNavFor;
    private attachEvents;
    private onNavReady;
    private onTargetReady;
    private onNavClick;
    private onNavTouch;
    private onNavCreateSlide;
    private onTargetChange;
    private markSelectedSlide;
    attach(): void;
    detach(): void;
}
export {};
