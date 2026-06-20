/**
 * An ES2015 spec-compliant `Function.prototype.name` getter implementation.
 *
 * @returns The name of the function it is called on.
 */
declare function getName(this: Function): string;

export = getName;
