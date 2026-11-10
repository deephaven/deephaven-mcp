import { styleText } from "node:util";
import type { ConsoleInfo } from "../../actions/console/enterprise.ts";
import { consoleStateDetails } from "../../actions/console/reporting.ts";

/** Format the console name and stable ID as a detail heading. */
export function consoleHeading(console: ConsoleInfo): string {
  return `${styleText("bold", console.name)} (${styleText("cyan", console.id)})`;
}

/** Format a labeled worker state with its indented explanation. */
export function consoleStateLines(state: string): string[] {
  return [`  State: ${coloredState(state)}`, ...stateExplanationLines(state)];
}

/** Format one list entry, including selection and any worker-state explanation. */
export function consoleListLines(
  console: ConsoleInfo & { inUse: boolean },
): string[] {
  const marker = console.inUse ? styleText("green", "●") : " ";
  const shortcode = styleText("cyan", console.shortcode);
  return [
    `${marker} ${shortcode}  ${coloredState(console.state)}  ${console.name}`,
    ...stateExplanationLines(console.state),
  ];
}

/** Highlight unavailable status without classifying other server states. */
function coloredState(state: string): string {
  const color = state === "Unknown" ? "yellow" : "cyan";
  return styleText(color, state);
}

/** Visually nest explanatory text below the state it describes. */
function stateExplanationLines(state: string): string[] {
  return consoleStateDetails(state).map(
    (line) => `    ${styleText("dim", line)}`,
  );
}
