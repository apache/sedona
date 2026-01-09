/**
 * Returns a throttled version of a given function that is only invoked at most
 * once within a given threshold of time in milliseconds.
 */
export declare const throttle: (func: Function, timeout: number) => (...args: any) => void;
