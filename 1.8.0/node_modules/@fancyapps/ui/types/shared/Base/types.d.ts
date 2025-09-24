export type Constructor<T = any> = new (...args: any[]) => T;
export type Listener = (...args: any[]) => void;
export type DeepKeyOf<O> = {
    [K in Extract<keyof O, string>]: O[K] extends Array<any> ? K : O[K] extends Record<string, unknown> ? `${K}` | `${K}.${DeepKeyOf<O[K]>}` : K;
}[Extract<keyof O, string>];
export type DeepGet<O, P extends string> = P extends `${infer Key}.${infer Rest}` ? Key extends keyof O | `${bigint}` ? DeepGet<O[Key & keyof O], Rest> : never : P extends keyof O | `${bigint}` ? O[P & keyof O] : never;
export type ComponentEventsType = "*" | "attachPlugins" | "detachPlugins";
export type PluginEventsType = "*";
