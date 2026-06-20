import type names from 'possible-typed-array-names';
import type { TypedArray } from 'is-typed-array';

declare function typedArrayLength(value: typedArrayLength.TypedArray): number;
declare function typedArrayLength(value: unknown): false;

declare namespace typedArrayLength {
	export type { TypedArray };

	export type TypedArrayName = typeof names[number];
}

export = typedArrayLength;
