export declare enum States {
    Init = 0,
    Ready = 1,
    Destroy = 2
}
import { userSlideType, slideType, pageType } from "./types";
export declare const createSlide: (params: userSlideType) => slideType;
export declare const createPage: (parameter?: Partial<pageType>) => pageType;
