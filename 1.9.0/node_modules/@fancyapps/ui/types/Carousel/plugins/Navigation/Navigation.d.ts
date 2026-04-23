import { Plugin } from "../../../shared/Base/Plugin";
import { Carousel } from "../../Carousel";
export type NavigationOptionsType = {
    /**
     * Class names for DOM elements
     */
    classes: {
        container: string;
        button: string;
        isNext: string;
        isPrev: string;
    };
    /**
     * HTML template for left arrow
     */
    nextTpl: string;
    /**
     * HTML template for right arrow
     */
    prevTpl: string;
};
declare module "../../../Carousel/options" {
    interface PluginsOptionsType {
        Navigation: Boolean | Partial<NavigationOptionsType>;
    }
}
type NavigationEventsType = "";
export declare class Navigation extends Plugin<Carousel, NavigationOptionsType, NavigationEventsType> {
    static defaults: NavigationOptionsType;
    container: HTMLElement | null;
    prev: HTMLElement | null;
    next: HTMLElement | null;
    private isDom;
    private onRefresh;
    private addBtn;
    private build;
    cleanup(): void;
    attach(): void;
    detach(): void;
}
export {};
