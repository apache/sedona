import { PropertyKey } from './isPropertyKey';

declare function Get<
    T extends object,
    K extends keyof T & PropertyKey,
>(
    O: T,
    P: K,
): T[keyof T] | undefined;

export = Get;
