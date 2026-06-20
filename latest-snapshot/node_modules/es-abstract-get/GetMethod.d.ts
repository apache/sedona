import { PropertyKey } from './isPropertyKey';

type GoodMethodKey<O, K extends PropertyKey> =
    K extends keyof O
        ? (O[K] extends Function | null | undefined ? K : never)
        : K;

declare function GetMethod<O extends {}, K extends PropertyKey>(
    V: O,
    P: K & GoodMethodKey<O, K>,
): Function | undefined;

export = GetMethod;
