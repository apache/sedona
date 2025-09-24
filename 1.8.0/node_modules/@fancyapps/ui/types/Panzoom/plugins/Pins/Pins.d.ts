import { Panzoom } from "../../../Panzoom/Panzoom";
import { Plugin } from "../../../shared/Base/Plugin";
declare module "../../../Panzoom/options" {
    interface PluginsOptionsType {
        Pins?: Boolean;
    }
}
type PinsOptionsType = {};
type PinsEventsType = "";
export declare class Pins extends Plugin<Panzoom, PinsOptionsType, PinsEventsType> {
    static defaults: {};
    container: HTMLElement | null;
    pins: Array<HTMLElement>;
    onTransform(instance: Panzoom): void;
    attach(): void;
    detach(): void;
}
export {};
