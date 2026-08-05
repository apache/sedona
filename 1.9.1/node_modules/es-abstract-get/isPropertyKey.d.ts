declare function isPropertyKey(
    argument: unknown,
): argument is isPropertyKey.PropertyKey;

declare namespace isPropertyKey {
    export type PropertyKey = string | symbol;
}

export = isPropertyKey;
