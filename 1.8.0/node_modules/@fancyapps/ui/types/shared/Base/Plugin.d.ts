import { Base } from "./Base";
export declare class Plugin<ComponentType, PluginOptionsType extends object, PluginEventsType extends string> extends Base<PluginOptionsType, PluginEventsType> {
    instance: ComponentType;
    constructor(instance: ComponentType, options: PluginOptionsType);
    attach(): void;
    detach(): void;
}
