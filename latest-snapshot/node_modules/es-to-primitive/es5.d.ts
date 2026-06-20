import type { primitive } from './';

declare function ToPrimitive(
    input: ToPrimitive.unknownES5,
    hint?: StringConstructor | NumberConstructor,
): ToPrimitive.primitiveES5;

declare namespace ToPrimitive {
    export type primitiveES5 = Exclude<primitive, symbol | bigint>;
    export type unknownES5 = primitiveES5 | object;
}

export = ToPrimitive;
