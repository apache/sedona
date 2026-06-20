import type { primitive } from './';

declare function ToPrimitive(
    input: unknown,
    hint?: StringConstructor | NumberConstructor,
): primitive;

export = ToPrimitive;
