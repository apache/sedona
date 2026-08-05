import { Constructor } from "../shared/Base/types";
import { Plugin } from "../shared/Base/Plugin";
import { Fancybox } from "./Fancybox";
export type PluginsType = Record<string, Constructor<Plugin<Fancybox, any, any>>>;
import { CarouselEventsType } from "../Carousel/types";
import { ComponentEventsType } from "../shared/Base/types";
export type EventsType = ComponentEventsType
/**
 * Initialization has started, plugins have been added
 */
 | "init"
/**
 * The layout is initialized
 */
 | "initLayout"
/**
 * The Carousel is initialized
 */
 | "initCarousel"
/**
 * Initialization has been completed
 */
 | "ready"
/**
 * The content on one of the slides starts loading
 */
 | "loading"
/**
 * Content is loaded on one of the slides but is not yet displayed
 */
 | "loaded"
/**
 * Content could not be loaded in one of the slides
 */
 | "error"
/**
 * Content is ready to be displayed on one of the slides
 */
 | "reveal"
/**
 * Content is revealed on one of the slides
 */
 | "done"
/**
 * Cleared content inside the slide
 */
 | "clearContent"
/**
 * The idle state is activated
 */
 | "setIdle"
/**
 * The idle state is deactivated
 */
 | "endIdle"
/**
 * The slideshow has been deactivated
 */
 | "endSlideshow"
/**
 * A keyboard button is pressed
 */
 | "keydown"
/**
 * Single click event has been detected
 */
 | "click"
/**
 * Wheel event has been detected
 */
 | "wheel"
/**
 * A window resizing event was detected
 */
 | "resize"
/**
 * Closing has begun and can be prevented
 */
 | "shouldClose"
/**
 * The slideshow is activated
 */
 | "startSlideshow"
/**
 * Closing is ongoing
 */
 | "close"
/**
 * Closing is complete
 */
 | "destroy" | `Carousel.${CarouselEventsType}`;
export type Events = Record<EventsType, (...args: any[]) => void>;
