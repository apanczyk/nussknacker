import React, { MouseEventHandler } from "react";
import { Link } from "react-router-dom";
import { css, cx } from "@emotion/css";
import { NodeId } from "../../../types";
import Color from "color";
import { useTheme } from "@mui/material";

export const NodeErrorLink = (props: { onClick: MouseEventHandler<HTMLAnchorElement>; nodeId: NodeId; disabled?: boolean }) => {
    const { onClick, nodeId, disabled } = props;
    const theme = useTheme();

    const styles = css({
        whiteSpace: "normal",
        fontWeight: 600,
        color: theme.custom.colors.error,
        "a&": {
            "&:hover": {
                color: Color(theme.custom.colors.error).lighten(0.25).hex(),
            },
            "&:focus": {
                color: theme.custom.colors.error,
                textDecoration: "none",
            },
        },
    });

    return disabled ? (
        <span
            className={cx(
                styles,
                css({
                    color: Color(theme.custom.colors.error).desaturate(0.5).lighten(0.1).hex(),
                }),
            )}
        >
            {nodeId}
        </span>
    ) : (
        <Link className={styles} to={`?nodeId=${nodeId}`} onClick={onClick}>
            {/* blank values don't render as links so this is a workaround */}
            {nodeId.trim() === "" ? "blank" : nodeId}
        </Link>
    );
};
