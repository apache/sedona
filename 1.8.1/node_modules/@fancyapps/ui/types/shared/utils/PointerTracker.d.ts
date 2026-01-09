export interface Point {
    clientX: number;
    clientY: number;
}
export declare class Pointer {
    pageX: number;
    pageY: number;
    clientX: number;
    clientY: number;
    id: number;
    time: number;
    nativePointer: Touch | MouseEvent;
    constructor(nativePointer: Touch | MouseEvent);
}
export type InputEvent = TouchEvent | MouseEvent;
type StartCallback = (event: InputEvent, pointer: Pointer, currentPointers: Pointer[]) => boolean;
type MoveCallback = (event: InputEvent, changedPointers: Pointer[], previousPointers: Pointer[]) => void;
type EndCallback = (event: InputEvent, pointer: Pointer, currentPointers: Pointer[]) => void;
interface PointerTrackerOptions {
    start: StartCallback;
    move?: MoveCallback;
    end?: EndCallback;
}
export declare class PointerTracker {
    private element;
    private startCallback;
    private moveCallback;
    private endCallback;
    readonly currentPointers: Pointer[];
    readonly startPointers: Pointer[];
    constructor(element: HTMLElement, { start, move, end, }: PointerTrackerOptions);
    private onPointerStart;
    private onTouchStart;
    private onMove;
    private onPointerEnd;
    private onTouchEnd;
    private triggerPointerStart;
    private triggerPointerEnd;
    private onWindowBlur;
    clear(): void;
    stop(): void;
}
export declare function getDistance(a: Point, b?: Point): number;
export declare function getMidpoint(a: Point, b?: Point): Point;
export {};
