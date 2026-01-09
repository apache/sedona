import { Plugin } from "../../../shared/Base/Plugin";
import { ThumbsOptionsType as CarouselThumbsOptionsType } from "../../../Carousel/plugins/Thumbs/Thumbs";
import { Fancybox } from "../../Fancybox";
type OptionsType = CarouselThumbsOptionsType & {
    /**
     *  Keyboard shortcut to toggle thumbnail container
     */
    key: string | false;
    /**
     * Change the location where the thumbnail container is added
     */
    parentEl: HTMLElement | null | (() => HTMLElement | null);
    /**
     * If thumbnail bar should appear automatically after Fancybox is launched
     */
    showOnStart: boolean;
};
export type ThumbsOptionsType = Partial<OptionsType>;
declare module "../../../Fancybox/options" {
    interface PluginsOptionsType {
        Thumbs: Boolean | Partial<ThumbsOptionsType>;
    }
}
export declare class Thumbs extends Plugin<Fancybox, ThumbsOptionsType, ""> {
    static defaults: OptionsType;
    private ref;
    private hidden;
    get isEnabled(): boolean;
    get isHidden(): boolean;
    private onClick;
    private onCreateSlide;
    private onInit;
    private onResize;
    private onKeydown;
    toggle(): void;
    show(): void;
    hide(): void;
    refresh(): void;
    attach(): void;
    detach(): void;
}
export {};
