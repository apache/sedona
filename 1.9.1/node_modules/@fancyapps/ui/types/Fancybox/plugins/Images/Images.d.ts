import { Plugin } from "../../../shared/Base/Plugin";
import { Panzoom } from "../../../Panzoom/Panzoom";
import { OptionsType as PanzoomOptionsType } from "../../../Panzoom/options";
import { Carousel } from "../../../Carousel/Carousel";
import { slideType } from "../../../Carousel/types";
import { Fancybox } from "../../Fancybox";
export type OptionsType = {
    /**
     * Set custom content per slide
     */
    content?: (instance: Images, slide: slideType) => string | HTMLElement | HTMLPictureElement;
    /**
     * Initial image zoom level, see Panzoom documentation for more information.
     */
    initialSize: "fit" | "cover" | "full" | "max" | ((instance: Images) => "fit" | "cover" | "full" | "max");
    /**
     * Custom options for Panzoom instance, see Panzoom documentation for more information.
     */
    Panzoom: Partial<PanzoomOptionsType>;
    /**
     * If the image download needs to be prevented
     */
    protected: boolean;
    /**
     * If animate an image with zoom in/out animation when launching/closing Fancybox
     */
    zoom: boolean;
    /**
     * If zoom animation should animate the opacity when launching/closing Fancybox
     */
    zoomOpacity: "auto" | boolean;
};
declare module "../../../Carousel/types" {
    interface slideType {
        panzoom?: Panzoom;
        imageEl?: HTMLImageElement | HTMLPictureElement;
        srcset?: string;
        sizes?: string;
        media?: string;
        sources?: string;
    }
}
declare module "../../../Fancybox/options" {
    interface PluginsOptionsType {
        Images: Boolean | Partial<ImagesOptionsType>;
    }
}
export type ImagesOptionsType = Partial<OptionsType>;
export declare class Images extends Plugin<Fancybox, ImagesOptionsType, ""> {
    static defaults: OptionsType;
    onCreateSlide(_fancybox: Fancybox, _carousel: Carousel, slide: slideType): void;
    onRemoveSlide(_fancybox: Fancybox, _carousel: Carousel, slide: slideType): void;
    onChange(_fancybox: Fancybox, carousel: Carousel, page: number, _prevPage: number): void;
    onClose(): void;
    setImage(slide: slideType, imageSrc: string): void;
    process(slide: slideType, imageSrc: string): Promise<Panzoom>;
    zoomIn(slide: slideType): Promise<Panzoom>;
    getZoomInfo(slide: slideType): false | {
        x: number;
        y: number;
        scale: number;
        opacity: boolean;
    };
    attach(): void;
    detach(): void;
}
