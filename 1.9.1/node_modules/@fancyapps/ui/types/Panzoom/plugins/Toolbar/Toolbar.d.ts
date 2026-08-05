import { Plugin } from "../../../shared/Base/Plugin";
import { Panzoom } from "../../Panzoom";
import { PanzoomButtons } from "../../../shared/buttons";
export type ToolbarItemType = {
    icon: string;
    action?: string;
    change?: Record<string, number | boolean>;
    click?: (instance: Panzoom) => void;
    title?: string;
};
export type ToolbarOptionsType = {
    display: Array<keyof typeof PanzoomButtons>;
    items: Record<string, ToolbarItemType>;
    svgAttr: Record<string, string>;
};
declare module "../../../Panzoom/options" {
    interface PluginsOptionsType {
        Toolbar?: Boolean | Partial<ToolbarOptionsType>;
    }
}
type ToolbarEventsType = "";
export declare class Toolbar extends Plugin<Panzoom, ToolbarOptionsType, ToolbarEventsType> {
    static defaults: ToolbarOptionsType;
    container: HTMLElement | null;
    addItem(key: string): void;
    createContainer(): void;
    removeContainer(): void;
    attach(): void;
    detach(): void;
}
export {};
