import { css, cx } from "@emotion/css";
import { WindowButtonProps, WindowContentProps } from "@touk/window-manager";
import React, { PropsWithChildren, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { PromptContent, WindowKind } from "../../windowManager";

export interface InfoDialogData {
    text: string;
    confirmText?: string;
}

export function GenericInfoDialog({ children, ...props }: PropsWithChildren<WindowContentProps<WindowKind, InfoDialogData>>): JSX.Element {
    const dialogData: InfoDialogData = props.data.meta;

    const { t } = useTranslation();
    const buttons: WindowButtonProps[] = useMemo(
        () => [
            {
                title: dialogData.confirmText || t("dialog.button.info.confirm", "Ok"),
                action: () => props.close(),
            },
        ],
        [dialogData, props, t],
    );

    return (
        <PromptContent {...props} buttons={buttons}>
            <div className={cx("modalContentDark", css({ minWidth: 400 }))}>
                <h3 className={css({ textAlign: "center" })}>{dialogData.text}</h3>
                {children}
            </div>
        </PromptContent>
    );
}

export default GenericInfoDialog;
