import { MatrixKeys } from "./consts";
export type MatrixValues = {
    [K in (typeof MatrixKeys)[number]]: number;
};
export type Bounds = {
    x: {
        min: number;
        max: number;
    };
    y: {
        min: number;
        max: number;
    };
};
import { ComponentEventsType } from "../shared/Base/types";
export type EventsType = ComponentEventsType
/**
 * Initialization has started, plugins have been added
 */
 | "init"
/**
 * Content is loading
 */
 | "beforeLoad"
/**
 * Content has loaded successfully
 */
 | "afterLoad"
/**
 * Content did not load successfully
 */
 | "error"
/**
 * Panzoom has successfully launched
 */
 | "ready"
/**
 * Single click event has been detected
 */
 | "click"
/**
 * Double click event has been detected
 */
 | "dblClick"
/**
 * Wheel event has been detected
 */
 | "wheel"
/**
 * Container and content dimensions have been updated
 */
 | "refresh"
/**
 * Pointer down event has been detected
 */
 | "touchStart"
/**
 * Pointer move event has been detected
 */
 | "touchMove"
/**
 * Pointer up/cancel event has been detected
 */
 | "touchEnd"
/**
 * Deceleration animation has started
 */
 | "decel"
/**
 * Mouse move event has been detected
 */
 | "mouseMove"
/**
 * Animation has started
 */
 | "startAnimation"
/**
 * Animation has ended
 */
 | "endAnimation"
/**
 * The "transform" CSS property of the content will be updated.
 */
 | "beforeTransform"
/**
 * The "transform" CSS property of the content has been updated
 */
 | "afterTransform"
/**
 * Enter full-screen mode
 */
 | "enterFS"
/**
 * Exit full-screen mode
 */
 | "exitFS"
/**
 * Instance is detroyed
 */
 | "destroy";
export type Events = Record<EventsType, (...args: any[]) => void>;
import { Constructor } from "../shared/Base/types";
import { Plugin } from "../shared/Base/Plugin";
import { Panzoom } from "./Panzoom";
export type PluginsType = Record<string, Constructor<Plugin<Panzoom, any, any>>>;
export type ClickAction = "toggleZoom" | "toggleCover" | "toggleMax" | "zoomToFit" | "zoomToMax" | "iterateZoom" | false;
export type WheelAction = "zoom" | "pan" | false;
export type ZoomOptions = {
    friction?: number | "auto";
    originX?: number | "auto";
    originY?: number | "auto";
    event?: Event & MouseEvent;
};
