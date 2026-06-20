/**
 * Determines the type of the given collection, or returns false.
 *
 * @param {unknown} value The potential collection
 * @returns {TypedArrayName | false | null} 'Int8Array' | 'Uint8Array' | 'Uint8ClampedArray' | 'Int16Array' | 'Uint16Array' | 'Int32Array' | 'Uint32Array' | 'Float32Array' | 'Float64Array' | 'BigInt64Array' | 'BigUint64Array' | false | null
 */
declare function whichTypedArray<T>(value: T): false | null | whichTypedArray.WhichTypedArray<T>;
declare function whichTypedArray(value: unknown): false | null | whichTypedArray.TypedArrayName;

import TAs from 'available-typed-arrays';

declare namespace whichTypedArray {
	export type TypedArrayName = ReturnType<typeof TAs>[number];

	export type TypedArrayConstructor = typeof globalThis[TypedArrayName];

	export type TypedArray = TypedArrayConstructor['prototype'];

	/**
	 * Distributes over `T`, so a subset of typed arrays maps to the matching
	 * subset of names (`Int8Array | Uint8Array` -> `'Int8Array' | 'Uint8Array'`,
	 * never a float16 or bigint name). Any non-typed-array part adds `false | null`.
	 *
	 * Derived entirely from `TypedArrayName`, so a new entry in `available-typed-arrays` flows through with no other change here.
	 */
	export type WhichTypedArray<T> =
		| {
			[Name in TypedArrayName]: T extends typeof globalThis[Name]['prototype']
				? Name
				: never
		}[TypedArrayName]
		| ([T] extends [TypedArray] ? never : false | null);
}

export = whichTypedArray;
