import { DeepGet, DeepKeyOf, Listener } from "./types";
export declare class Base<OptionsType, EventsType> {
    options: Partial<OptionsType>;
    static version: string;
    static defaults: unknown;
    protected events: Map<EventsType, Array<Listener>>;
    constructor(options?: Partial<OptionsType>);
    setOptions(options: Partial<OptionsType>): void;
    option<T extends DeepKeyOf<OptionsType>>(key: T, ...rest: any): Exclude<DeepGet<OptionsType, T>, Function>;
    optionFor<T extends DeepKeyOf<OptionsType>>(obj: any, key: T, fallback?: DeepGet<OptionsType, T>, ...rest: any): Exclude<DeepGet<OptionsType, T>, Function>;
    cn(key: string): string;
    localize(str: string, params?: Array<[string, any]>): string;
    on(what: EventsType | EventsType[] | "*", listener: Listener): void;
    off(what: EventsType | EventsType[] | "*", listener: Listener): void;
    emit(event: EventsType, ...args: any[]): void;
}
