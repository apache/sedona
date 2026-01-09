import { Plugin } from "../../../shared/Base/Plugin";
import { PanzoomButtonsType } from "../../../shared/buttons";
import { Fancybox } from "../../Fancybox";
export declare enum ToolbarStates {
    Init = 0,
    Ready = 1,
    Disabled = 2
}
export declare const ToolbarItems: {
    infobar: {
        tpl: string;
    };
    download: {
        tpl: string;
    };
    prev: {
        tpl: string;
    };
    next: {
        tpl: string;
    };
    slideshow: {
        tpl: string;
    };
    fullscreen: {
        tpl: string;
    };
    thumbs: {
        tpl: string;
    };
    close: {
        tpl: string;
    };
};
export type ToolbarItemType = {
    tpl: string;
    click?: (instance: Toolbar, event: Event) => void;
};
export type ToolbarItemsType = Record<keyof typeof ToolbarItems | string, ToolbarItemType>;
export type ToolbarItemKey = keyof PanzoomButtonsType | keyof ToolbarItemsType;
export type ToolbarPosition = "left" | "middle" | "right";
type OptionsType = {
    /**
     * If absolutely position container;
     * "auto" - absolutely positioned if there is no item in the middle column.
     */
    absolute: "auto" | boolean;
    /**
     * What toolbar items to display
     */
    display: Record<ToolbarPosition, Array<ToolbarItemKey>>;
    /**
     * If enabled; "auto" - enable only if there is at least one image in the gallery
     */
    enabled: "auto" | boolean;
    /**
     * Collection of all available toolbar items
     */
    items: ToolbarItemsType;
    /**
     * Change where toolbar container is appended
     */
    parentEl: HTMLElement | null | ((toolbar: Toolbar) => HTMLElement | null);
};
export type ToolbarOptionsType = Partial<OptionsType>;
declare module "../../../Fancybox/options" {
    interface PluginsOptionsType {
        Toolbar: Boolean | Partial<ToolbarOptionsType>;
    }
}
export declare class Toolbar extends Plugin<Fancybox, ToolbarOptionsType, ""> {
    static defaults: OptionsType;
    state: ToolbarStates;
    private container;
    private onReady;
    private onClick;
    private onChange;
    private onRefresh;
    private onDone;
    private createContainer;
    private createEl;
    private removeContainer;
    attach(): void;
    detach(): void;
}
export {};
