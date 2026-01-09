import { Plugin } from "../../../shared/Base/Plugin";
import { Carousel } from "../../Carousel";
export type DotsOptionsType = {
    classes: {
        list: string;
        dot: string;
        isDynamic: string;
        hasDots: string;
        isBeforePrev: string;
        isPrev: string;
        isCurrent: string;
        isNext: string;
        isAfterNext: string;
    };
    dotTpl: string;
    dynamicFrom: number | false;
    maxCount: number;
    minCount: number;
};
declare module "../../../Carousel/options" {
    interface PluginsOptionsType {
        Dots?: Boolean | Partial<DotsOptionsType>;
    }
}
type DotsEventsType = "";
export declare class Dots extends Plugin<Carousel, DotsOptionsType, DotsEventsType> {
    static defaults: DotsOptionsType;
    /**
     * If this instance is currently in "dynamic" mode
     */
    isDynamic: boolean;
    private list;
    private onRefresh;
    private build;
    /**
     * Refresh the elements to match the current state of the carousel
     */
    refresh(): void;
    private createItem;
    private cleanup;
    attach(): void;
    detach(): void;
}
export {};
