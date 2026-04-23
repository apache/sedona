import { Constructor } from "./types";
import { Base } from "./Base";
import { Plugin } from "./Plugin";
export declare class Component<ComponentOptionsType, ComponentEventsType> extends Base<ComponentOptionsType, ComponentEventsType> {
    plugins: Record<string, Plugin<Component<ComponentOptionsType, ComponentEventsType>, any, any>>;
    constructor(options?: Partial<ComponentOptionsType>);
    attachPlugins(Plugins?: Record<string, Constructor<Plugin<Component<ComponentOptionsType, ComponentEventsType>, any, any>>>): void;
    detachPlugins(Plugins?: string[]): this;
}
