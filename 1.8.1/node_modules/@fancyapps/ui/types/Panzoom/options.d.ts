import { Bounds, Events, ClickAction, WheelAction } from "./types";
export interface PluginsOptionsType {
}
export interface classesType {
    /**
     * Class name that specifies the content
     */
    content: string;
    /**
     * Content is currently loading
     */
    isLoading: string;
    /**
     * Content can be zoomed in
     */
    canZoomIn: string;
    /**
     * Content can be zoomed out
     */
    canZoomOut: string;
    /**
     * Content is draggable
     */
    isDraggable: string;
    /**
     * User is currently dragging
     */
    isDragging: string;
    /**
     * Container is in the fullscreen mode
     */
    inFullscreen: string;
    /**
     * Class name applied to `html` element indicating that at least one container is in full screen mode
     */
    htmlHasFullscreen: string;
}
export interface ComponentOptionsType {
    /**
     * Specify the content element. If no content is specified, it will be assumed that the content is the first child element
     */
    content: Element | null | ((...any: any) => Element | null);
    /**
     * Content width
     */
    width: "auto" | number | ((...any: any) => "auto" | number);
    /**
     * Content height
     */
    height: "auto" | number | ((...any: any) => "auto" | number);
    /**
     * Use touch events to pan content or follow the cursor.
     * Automatically switches to `drag` if user’s primary input mechanism can not hover over elements.
     * Tip: experiment with `mouseMoveFactor` option for better ux.
     */
    panMode: "drag" | "mousemove";
    /**
     * Enable touch guestures
     */
    touch: boolean | ((...any: any) => boolean);
    /**
     * If a CSS transformation is to be applied to the parent element of the content
     */
    transformParent: boolean;
    /**
     * Minimum touch drag distance to start panning content;
     * it can help detect click events on touch devices
     */
    dragMinThreshold: number;
    /**
     * Lock axis while dragging
     */
    lockAxis: "x" | "y" | "xy" | false;
    /**
     * The proportion by which the content pans relative to the cursor position;
     * for example, `2` means the cursor only needs to move 80% of the width/height of the content to reach the container border
     */
    mouseMoveFactor: number;
    /**
     * Animation friction when content is moved depending on cursor position
     */
    mouseMoveFriction: number;
    /**
     * Globally enable (or disable) any zooming
     */
    zoom: boolean | ((...any: any) => boolean);
    /**
     * Enable pinch-to-zoom guesture to zoom in/out using two fingers
     */
    pinchToZoom: boolean | ((...any: any) => boolean);
    /**
     * Allow panning only when content is zoomed in.
     * Using `true` allows other components (e.g. Carousel) to pick up touch events.
     * Note: if set to "auto", disables for touch devices (to allow page scrolling).
     */
    panOnlyZoomed: boolean | "auto" | ((...any: any) => boolean | "auto");
    /**
     * Minimum scale level
     */
    minScale: number | ((...any: any) => number);
    /**
     * The maximum zoom level the user can zoom.
     * If, for example, it is `2`, then the user can zoom content to 2x the original size
     */
    maxScale: number | ((...any: any) => number);
    /**
     * Default friction for animations, the value must be in the interval [0, 1)
     */
    friction: number;
    /**
     * Friction while panning/dragging
     */
    dragFriction: number;
    /**
     * Friction while decelerating after drag end
     */
    decelFriction: number;
    /**
     * Add click event listener
     */
    click: ClickAction | ((...any: any) => ClickAction);
    /**
     * Add double click event listener
     */
    dblClick: ClickAction | ((...any: any) => ClickAction);
    /**
     * Add mouse wheel event listener
     */
    wheel: WheelAction | ((...any: any) => WheelAction);
    /**
     * Number of times to stop restricting wheel operation after min/max zoom level is reached
     */
    wheelLimit: number;
    /**
     * Content x/y boundaries
     */
    bounds: "auto" | ((...any: any) => Bounds);
    /**
     * Force to ignore boundaries boundar; always or only when the drag is locked on the axis
     */
    infinite: boolean | "x" | "y";
    /**
     * If enable rubberband effect - drag out of bounds with resistance
     */
    rubberband: boolean;
    /**
     * Show loading icon
     */
    spinner: boolean;
    /**
     * Bounce after hitting the edge
     */
    bounce: boolean;
    /**
     * Limit the animation's maximum acceleration
     */
    maxVelocity: number | ((...any: any) => number);
    /**
     * Class names for DOM elements
     */
    classes: classesType;
    /**
     * Localization of strings
     */
    l10n?: Record<string, string>;
    /**
     * Optional event listeners
     */
    on?: Partial<Events>;
}
export declare const defaultOptions: ComponentOptionsType;
export type OptionsType = PluginsOptionsType & ComponentOptionsType;
