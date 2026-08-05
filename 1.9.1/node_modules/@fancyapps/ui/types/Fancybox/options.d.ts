import { Events } from "./types";
import { Fancybox } from "./Fancybox";
import { slideType } from "../Carousel/types";
import { OptionsType as CarouselOptionsType } from "../Carousel/options";
import { ClickAction as PanzoomClickAction } from "../Panzoom/types";
export interface PluginsOptionsType {
}
export type ClickAction = PanzoomClickAction | "close" | "next" | "prev";
export type keyboardAction = "close" | "next" | "prev";
export type keyboardType = {
    Escape: keyboardAction;
    Delete: keyboardAction;
    Backspace: keyboardAction;
    PageUp: keyboardAction;
    PageDown: keyboardAction;
    ArrowUp: keyboardAction;
    ArrowDown: keyboardAction;
    ArrowRight: keyboardAction;
    ArrowLeft: keyboardAction;
};
export interface ComponentOptionsType {
    /**
     * Should backdrop and UI elements fade in/out on start/close
     */
    animated: boolean | ((instance: Fancybox) => boolean);
    /**
     * Set focus on first focusable element after displaying content
     */
    autoFocus: boolean | ((instance: Fancybox) => boolean);
    /**
     * The action to perform when the user clicks on the backdrop
     */
    backdropClick: ClickAction | ((instance: Fancybox) => ClickAction | void);
    /**
     * Change caption per slide
     */
    caption?: string | HTMLElement | false | ((instance: Fancybox, slide: slideType, caption?: string | HTMLElement) => string | HTMLElement | false);
    /**
     * Optional object to extend options for main Carousel
     */
    Carousel: Partial<CarouselOptionsType>;
    /**
     * If true, a close button will be created above the content
     */
    closeButton: "auto" | boolean;
    /**
     * If true, previously opened instance will be closed
     */
    closeExisting: boolean;
    /**
     * If true, only one caption element will be used for all slides
     */
    commonCaption: boolean;
    /**
     * If compact mode needs to be activated
     */
    compact: boolean | ((instance: Fancybox) => boolean);
    /**
     * The action to perform when the user clicks on the content
     */
    contentClick: ClickAction | ((instance: Fancybox) => ClickAction | void);
    /**
     * The action to take when the user double-clicks on the content
     */
    contentDblClick: ClickAction | ((instance: Fancybox) => ClickAction | void);
    /**
     * Default content type
     */
    defaultType: "image" | "iframe" | "youtube" | "vimeo" | "inline" | "html" | "ajax";
    /**
     * The default value of the CSS `display` property for hidden inline elements
     */
    defaultDisplay: "flex" | "block" | string;
    /**
     * Enable drag-to-close gesture - drag content up/down to close instance
     */
    dragToClose: boolean | ((instance: Fancybox) => boolean);
    /**
     * If Fancybox should start in full-scren mode
     */
    Fullscreen: {
        autoStart: boolean;
    };
    /**
     * If true, all matching elements will be grouped together in one group regardless of the value of `data-fancybox` attribute
     */
    groupAll: boolean;
    /**
     * The name of the attribute used for grouping
     */
    groupAttr: string;
    /**
     * Change content height per slide
     */
    height?: "auto" | number | ((instance: Fancybox, slide: slideType, origHeight: number) => "auto" | number);
    /**
     * Class name to be applied to the content to hide it.
     * Note: If you disable image zoom, this class name will be used to run the image hide animation.
     */
    hideClass: string | false | ((instance: Fancybox) => string | false);
    /**
     * If browser scrollbar should be hidden
     */
    hideScrollbar: boolean;
    /**
     * Custom `id` for the instance
     */
    id?: number | string;
    /**
     * Timeout in milliseconds after which to activate idle mode
     */
    idle: number | false;
    /**
     * Keyboard events
     */
    keyboard: keyboardType;
    /**
     * Localization of strings
     */
    l10n?: Record<string, string>;
    /**
     * Custom class name for the container
     */
    mainClass?: string;
    /**
     * Optional event listeners
     */
    on?: Partial<Events>;
    /**
     * Element where container is appended
     * Note. If no element is specified, container is appended to the `document.body`
     */
    parentEl?: HTMLElement | null;
    /**
     * Set focus back to trigger element after closing Fancybox
     */
    placeFocusBack: boolean;
    /**
     * Change source per slide
     */
    src?: string | HTMLElement | ((instance: Fancybox, slide: slideType) => string | HTMLElement);
    /**
     * Class name to be applied to the content to reveal it.
     * Note: If you disable image zoom, this class name will be used to run the image reveal animation.
     */
    showClass: string | false | ((instance: Fancybox) => string | false);
    /**
     * Index of active slide on the start
     */
    startIndex: number;
    /**
     * HTML templates for various elements
     */
    tpl: {
        closeButton?: string;
        main?: string;
    };
    /**
     *  Trap focus inside Fancybox
     */
    trapFocus: boolean;
    /**
     * Change content width per slide
     */
    width?: "auto" | number | ((instance: Fancybox, slide: slideType, origWidth: number) => "auto" | number);
    /**
     * Mouse wheel event listener
     */
    wheel: "zoom" | "pan" | "close" | "slide" | false | ((instance: Fancybox, event: MouseEvent) => "zoom" | "pan" | "close" | "slide" | false);
    event?: MouseEvent | undefined;
    triggerEl?: HTMLElement | null;
    delegate?: HTMLElement | null;
}
export declare const defaultOptions: ComponentOptionsType;
export type OptionsType = PluginsOptionsType & ComponentOptionsType;
