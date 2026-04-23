import { Plugin } from "../../../shared/Base/Plugin";
import { slideType } from "../../../Carousel/types";
import { Fancybox } from "../../Fancybox";
export type HtmlOptionsType = {
    /**
     * A body of data to be sent in the XHR request
     */
    ajax: Document | XMLHttpRequestBodyInit | null;
    /**
     * If resize the iframe element to match the dimensions of the iframe page content
     */
    autoSize: boolean;
    /**
     * Attributes of an iframe element
     */
    iframeAttr: Record<string, string>;
    /**
     * If wait for iframe content to load before displaying
     */
    preload: boolean;
    /**
     * If videos should start playing automatically after they are displayed
     */
    videoAutoplay: boolean;
    /**
     * HTML5 video element template
     */
    videoTpl: string;
    /**
     * Default HTML5 video format
     */
    videoFormat: string;
    /**
     * Aspect ratio of the video element
     */
    videoRatio: number;
    /**
     * Vimeo embedded player parameters
     * https://vimeo.zendesk.com/hc/en-us/articles/360001494447-Player-parameters-overview
     */
    vimeo: {
        byline: 0 | 1;
        color: string;
        controls: 0 | 1;
        dnt: 0 | 1;
        muted: 0 | 1;
    };
    /**
     * YouTube embedded player parameters
     * https://developers.google.com/youtube/player_parameters#Parameters
     */
    youtube: {
        controls?: 0 | 1;
        enablejsapi?: 0 | 1;
        nocookie?: 0 | 1;
        rel?: 0 | 1;
        fs?: 0 | 1;
    };
};
declare module "../../../Fancybox/options" {
    interface PluginsOptionsType {
        Html?: Boolean | Partial<HtmlOptionsType>;
    }
}
export declare const contentTypes: readonly ["image", "html", "ajax", "inline", "clone", "iframe", "map", "pdf", "html5video", "youtube", "vimeo"];
type contentType = (typeof contentTypes)[number];
declare module "../../../Carousel/types" {
    interface slideType {
        type?: contentType;
        defaultType?: contentType;
        ratio?: string | number;
        videoFormat?: string;
        ajax?: Document | XMLHttpRequestBodyInit | null;
        xhr?: XMLHttpRequest | null;
        poller?: ReturnType<typeof setTimeout>;
        placeholderEl?: HTMLElement | null;
        iframeEl?: HTMLIFrameElement | null;
        preload?: boolean;
        autoSize?: boolean;
        videoId?: string;
        autoplay?: boolean;
    }
}
export declare class Html extends Plugin<Fancybox, HtmlOptionsType, ""> {
    static defaults: HtmlOptionsType;
    private onBeforeInitSlide;
    private onCreateSlide;
    private onClearContent;
    private onSelectSlide;
    private onUnselectSlide;
    private onDone;
    private onRefresh;
    private onMessage;
    private loadAjaxContent;
    private setInlineContent;
    private setIframeContent;
    private resizeIframe;
    private playVideo;
    private processType;
    setContent(slide: slideType): void;
    private setAspectRatio;
    attach(): void;
    detach(): void;
}
export {};
