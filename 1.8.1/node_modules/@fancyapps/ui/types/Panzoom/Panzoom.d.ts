import { Component } from "../shared/Base/Component";
import { PluginsType, EventsType, Bounds, MatrixValues, ZoomOptions } from "./types";
import { States } from "./consts";
import { OptionsType } from "./options";
export type * from "./plugins/index";
export declare class Panzoom extends Component<OptionsType, EventsType> {
    static defaults: Partial<OptionsType>;
    static Plugins: PluginsType;
    private pointerTracker;
    private resizeObserver;
    private updateTimer;
    private clickTimer;
    private rAF;
    private isTicking;
    private ignoreBounds;
    private isBouncingX;
    private isBouncingY;
    private clicks;
    private trackingPoints;
    private pwt;
    private cwd;
    private pmme;
    private friction;
    /**
     * Current state of the instance
     */
    state: States;
    /**
     * True if the user is currently dragging
     */
    isDragging: boolean;
    /**
     * Reference to the container element
     */
    container: HTMLElement;
    /**
     * Reference to the content element
     */
    content: HTMLElement | HTMLImageElement | HTMLPictureElement;
    /**
     * Reference to the loading indicator element
     */
    spinner: HTMLElement | null;
    /**
     * Container dimensions.
     * `innerWidth` & `innerHeight` values are width/height without padding.
     */
    containerRect: {
        width: number;
        height: number;
        innerWidth: number;
        innerHeight: number;
    };
    /**
     * Content dimensions and position relative to parent.
     * `width` & `height` values are in current scale.
     */
    contentRect: {
        top: number;
        right: number;
        bottom: number;
        left: number;
        fullWidth: number;
        fullHeight: number;
        fitWidth: number;
        fitHeight: number;
        width: number;
        height: number;
    };
    /**
     * Initial position of the drag
     */
    dragStart: {
        x: number;
        y: number;
        top: number;
        left: number;
        time: number;
    };
    /**
     * Current position of the drag
     */
    dragOffset: {
        x: number;
        y: number;
        time: number;
    };
    /**
     * Transformation matrix values that define current x/y position and scale
     */
    current: MatrixValues;
    /**
     * Transformation matrix values that define the x/y position and scale of the end of the current animation
     */
    target: MatrixValues;
    /**
     * The velocity of each transformation matrix value
     */
    velocity: MatrixValues;
    /**
     * Axis that is currently locked
     */
    lockedAxis: false | "x" | "y";
    get fits(): boolean;
    /**
     * True if the user is on a touch device
     */
    get isTouchDevice(): boolean;
    get isMobile(): boolean;
    /**
     * Current pan mode - `mousemove` or `drag`
     */
    get panMode(): "mousemove" | "drag";
    /**
     * If the content can be dragged when it fits into the container
     */
    get panOnlyZoomed(): boolean | ((...any: any) => boolean | "auto");
    /**
     * True if the content has any boundaries
     */
    get isInfinite(): boolean | "x" | "y";
    /**
     * At what angle the content is rotated
     */
    get angle(): number;
    /**
     * At what angle the content is rotated at the end of the current animation
     */
    get targetAngle(): number;
    /**
     * At what scale the content is scaled
     */
    get scale(): number;
    /**
     * At what scale the content is scaled at the end of the current animation
     */
    get targetScale(): number;
    /**
     * Minimum scale level
     */
    get minScale(): number;
    /**
     * At what scale the content will be at full width and height (1 to 1)
     */
    get fullScale(): number;
    /**
     * Maximum scale level given the `maxScale` option
     */
    get maxScale(): number;
    /**
     * At what scale the content will cover the container
     */
    get coverScale(): number;
    /**
     * True if the content is currently animated and the scale level is changing
     */
    get isScaling(): boolean;
    /**
     * True if the content (image) has not finished loading
     */
    get isContentLoading(): boolean;
    /**
     * True if the animation is complete and the content fits within the bounds
     */
    get isResting(): boolean;
    constructor(container: HTMLElement | null, userOptions?: Partial<OptionsType>, userPlugins?: PluginsType);
    private initContent;
    private onLoad;
    private onError;
    private getNextScale;
    /**
     * Initialize the resize observer
     */
    attachObserver(): void;
    /**
     * Remove the resize observer
     */
    detachObserver(): void;
    /**
     * Enable click, wheel, mousemove and all touch events
     */
    attachEvents(): void;
    /**
     * Remove click, wheel, mouse movement and all touch events
     */
    detachEvents(): void;
    private animate;
    private setTargetForce;
    private checkBounds;
    private clampTargetBounds;
    private calculateContentDim;
    private setEdgeForce;
    private enable;
    private onClick;
    private addTrackingPoint;
    private onPointerDown;
    private onPointerMove;
    private onPointerUp;
    private startDecelAnim;
    private onWheel;
    private onMouseMove;
    private onKeydown;
    private onResize;
    private setTransform;
    /**
     * Calculate dimensions of contents and container
     */
    updateMetrics(silently?: boolean): void;
    /**
     * Calculate bounds based on the dimensions at the end of the current animation
     */
    calculateBounds(): Bounds;
    /**
     * Get information about current content boundaries
     */
    getBounds(): Bounds;
    /**
     * Enable or disable controls depending on the current size of the content
     */
    updateControls(): void;
    /**
     * Pan content to selected position and scale, use `friction` to control duration
     */
    panTo({ x, y, scale, friction, angle, originX, originY, flipX, flipY, ignoreBounds, }: {
        x?: number;
        y?: number;
        scale?: number;
        friction?: number;
        angle?: number;
        originX?: number;
        originY?: number;
        flipX?: boolean;
        flipY?: boolean;
        ignoreBounds?: boolean;
    }): void;
    /**
     * Relatively change position, scale, or angle of content, and flip content horizontally or vertically
     */
    applyChange({ panX, panY, scale, angle, originX, originY, friction, flipX, flipY, ignoreBounds, bounce, }: {
        panX?: number;
        panY?: number;
        scale?: number;
        angle?: number;
        originX?: number;
        originY?: number;
        friction?: number;
        flipX?: boolean;
        flipY?: boolean;
        ignoreBounds?: boolean;
        bounce?: boolean;
    }): void;
    /**
     * Stop animation and optionally set target values to the current values (or vice versa)
     */
    stop(freeze?: "current" | "target" | false): void;
    /**
     * Request an animation frame to check if the content position or scale needs to be changed based on the target values or if the content is out of bounds
     */
    requestTick(): void;
    /**
     * Update the current position based on the mouse move event
     */
    panWithMouse(event: MouseEvent, friction?: number): void;
    /**
     * Update the current scale based on the mouse scroll event
     */
    zoomWithWheel(event: WheelEvent): void;
    /**
     * Check if the content can be scaled up
     */
    canZoomIn(): boolean;
    /**
     * Check if the content can be scaled down
     */
    canZoomOut(): boolean;
    /**
     * Increase scale level
     */
    zoomIn(scale?: number, opts?: ZoomOptions): void;
    /**
     * Reduce scale level
     */
    zoomOut(scale?: number, opts?: ZoomOptions): void;
    /**
     * Change the scale level so that the content fits inside the container
     */
    zoomToFit(opts?: ZoomOptions): void;
    /**
     * Change the scale level so that the content covers the container
     */
    zoomToCover(opts?: ZoomOptions): void;
    /**
     * Change the scale level so that the content has full width and height (e.g, on a 1-to-1 scale)
     */
    zoomToFull(opts?: ZoomOptions): void;
    /**
     * Change the scale level to maximum
     */
    zoomToMax(opts?: ZoomOptions): void;
    /**
     * Toggle `full` scale level: `fit` -> `full` -> `fit`
     */
    toggleZoom(opts?: ZoomOptions): void;
    /**
     * Toggle scale level: `fit` -> `max` -> `fit`
     */
    toggleMax(opts?: ZoomOptions): void;
    /**
     * Toggle scale level: `fit` -> `cover` -> `fit`
     */
    toggleCover(opts?: ZoomOptions): void;
    /**
     * Iterate scale level: `fit` -> `full` -> `max` -> `fit`
     */
    iterateZoom(opts?: ZoomOptions): void;
    /**
     * Scale the content to a specific step or level.
     * Note: Origin `[0,0]` is in the center, because content has CSS `transform-origin: center center;`
     */
    zoomTo(zoomLevel?: number | "fit" | "cover" | "full" | "max" | "next", { friction, originX, originY, event, }?: ZoomOptions): void;
    /**
     * Rotate the content counterclockwise
     */
    rotateCCW(): void;
    /**
     * Rotate the content clockwise
     */
    rotateCW(): void;
    /**
     * Flip content horizontally
     */
    flipX(): void;
    /**
     * Flip content vertically
     */
    flipY(): void;
    /**
     * Change the scale level so that the content fits the container horizontally
     */
    fitX(): void;
    /**
     * Change the scale level so that the content fits the container vertically
     */
    fitY(): void;
    /**
     * Toggle full screen mode
     */
    toggleFS(): void;
    /**
     * Get transformation matrix values
     */
    getMatrix(source?: MatrixValues): DOMMatrix;
    /**
     * Reset all transformations AND animate to the original position
     */
    reset(friction?: number): void;
    /**
     * Destroy the instance
     */
    destroy(): void;
}
