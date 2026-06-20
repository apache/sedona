declare function GetV<T extends {}, K extends keyof T>(
    V: T,
    P: K,
): T[K];

declare function GetV<T extends {}, K extends keyof T>(
    V: T,
    P: PropertyKey,
): unknown;

export = GetV;
