// An Ink picker under Deno: arrows, Enter, q, Ctrl-C, and the non-TTY path.
// Usage: deno task ink              (interactive)
//        deno task ink < /dev/null  (non-TTY: must not enter raw mode)
import { Box, render, Text, useApp, useInput } from "ink";
import { useState } from "react";
import { runtime } from "./report.ts";

const items = [
  "https://dhe.example.com:8123",
  "http://localhost:10000",
  "Other…",
];

function Picker({ onPick }: { onPick: (value: string | null) => void }) {
  const [index, setIndex] = useState(0);
  const { exit } = useApp();
  useInput((input, key) => {
    if (key.upArrow) setIndex((i) => (i + items.length - 1) % items.length);
    if (key.downArrow) setIndex((i) => (i + 1) % items.length);
    if (key.return || input === "q") {
      onPick(key.return ? items[index] : null);
      exit();
    }
  });
  return (
    <Box flexDirection="column">
      <Text bold>? What server do you want to log in to?</Text>
      {items.map((item, i) => (
        <Text key={item} color={i === index ? "cyan" : undefined}>
          {i === index ? "❯ " : "  "}
          {item}
        </Text>
      ))}
    </Box>
  );
}

console.log(`ink (${runtime})`);

if (!Deno.stdin.isTerminal() || !Deno.stdout.isTerminal()) {
  console.log("PASS non-TTY detected; skipping Ink (flags would be required)");
  console.log("RESULT: PASS");
  Deno.exit(0);
}

let picked: string | null = null;
const start = performance.now();
const app = render(<Picker onPick={(v) => (picked = v)} />);
await app.waitUntilExit();
const ms = Math.round(performance.now() - start);
console.log(picked ? `PASS picked ${picked} (${ms} ms)` : "PASS cancelled");
console.log("RESULT: PASS");
Deno.exit(0);
