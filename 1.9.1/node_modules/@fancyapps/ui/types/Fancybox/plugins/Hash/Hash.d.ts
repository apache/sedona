import { Plugin } from "../../../shared/Base/Plugin";
import { Fancybox } from "../../Fancybox";
export type HashOptionsType = {
    slug?: string;
};
declare module "../../../Carousel/types" {
    interface slideType {
        slug?: string;
    }
}
declare module "../../../Fancybox/options" {
    interface PluginsOptionsType {
        Hash?: Boolean | Partial<HashOptionsType>;
        slug?: string;
    }
}
export declare const defaultOptions: HashOptionsType;
export declare class Hash extends Plugin<Fancybox, HashOptionsType, ""> {
    private onReady;
    private onChange;
    private onClose;
    attach(): void;
    detach(): void;
    static parseURL(): {
        hash: string;
        slug: string;
        index: number;
    };
    static startFromUrl(): void;
    static destroy(): void;
}
