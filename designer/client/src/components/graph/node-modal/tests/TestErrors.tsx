import React from "react";
import WarningIcon from "@mui/icons-material/Warning";
import NodeTip from "../NodeTip";
import { useTestResults } from "../TestResultsWrapper";
import { NodeTableBody } from "../NodeDetailsContent/NodeTable";
import { NodeRow } from "../NodeDetailsContent/NodeStyled";

export default function TestErrors(): JSX.Element {
    const results = useTestResults();

    if (!results.testResultsToShow?.error) {
        return null;
    }

    return (
        <NodeTableBody>
            <NodeRow>
                <div className="node-label">
                    <NodeTip title={"Test case error"} icon={<WarningIcon sx={(theme) => ({ color: theme.custom.colors.warning })} />} />
                </div>
                <div className="node-value">
                    <div className="node-error">
                        <>{results.testResultsToShow.error}</>
                    </div>
                </div>
            </NodeRow>
        </NodeTableBody>
    );
}
