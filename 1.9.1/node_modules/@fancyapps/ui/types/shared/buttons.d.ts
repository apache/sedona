export declare const PanzoomButtons: {
    panLeft: {
        icon: string;
        change: {
            panX: number;
        };
    };
    panRight: {
        icon: string;
        change: {
            panX: number;
        };
    };
    panUp: {
        icon: string;
        change: {
            panY: number;
        };
    };
    panDown: {
        icon: string;
        change: {
            panY: number;
        };
    };
    zoomIn: {
        icon: string;
        action: string;
    };
    zoomOut: {
        icon: string;
        action: string;
    };
    toggle1to1: {
        icon: string;
        action: string;
    };
    toggleZoom: {
        icon: string;
        action: string;
    };
    iterateZoom: {
        icon: string;
        action: string;
    };
    rotateCCW: {
        icon: string;
        action: string;
    };
    rotateCW: {
        icon: string;
        action: string;
    };
    flipX: {
        icon: string;
        action: string;
    };
    flipY: {
        icon: string;
        action: string;
    };
    fitX: {
        icon: string;
        action: string;
    };
    fitY: {
        icon: string;
        action: string;
    };
    reset: {
        icon: string;
        action: string;
    };
    toggleFS: {
        icon: string;
        action: string;
    };
};
export type PanzoomButtonType = {
    icon: string;
} & ({
    change: Record<string, number>;
    action?: never;
} | {
    action: string;
    change?: never;
});
export type PanzoomButtonsType = Record<keyof typeof PanzoomButtons, PanzoomButtonType>;
