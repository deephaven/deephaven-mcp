import process from "node:process";
import { Box, render, Text, useApp, useInput } from "ink";
import { type ReactNode, useEffect, useRef, useState } from "react";
import { DhError } from "../auth/errors.ts";
import type { Choice, Prompter } from "./prompt.ts";

type Finish<T> = (value: T) => void;

/** Renders one prompt on stderr; resolves with its value, or rejects on Ctrl-C. */
async function ask<T>(node: (finish: Finish<T>) => ReactNode): Promise<T> {
  let result: { value: T } | undefined;
  const app = render(node((value) => (result = { value })), {
    stdout: process.stderr,
    stdin: process.stdin,
    exitOnCtrlC: false,
    patchConsole: false,
  });
  await app.waitUntilExit();
  if (!result) throw new DhError("cancelled", "Cancelled");
  return result.value;
}

/** Shows the answer in place of the prompt, then unmounts. */
function useAnswer<T>(finish: Finish<T>) {
  const { exit } = useApp();
  const [answer, setAnswer] = useState<string>();
  useEffect(() => {
    if (answer !== undefined) exit();
  }, [answer]);
  return {
    answer,
    submit: (value: T, shown: string) => {
      finish(value);
      setAnswer(shown);
    },
    cancel: () => exit(),
  };
}

function Frame(
  { message, answer, hint, children }: {
    message: string;
    answer?: string;
    hint?: string;
    children?: ReactNode;
  },
) {
  if (answer !== undefined) {
    return (
      <Text>
        <Text color="green">✔</Text> {message}{" "}
        <Text color="cyan">{answer}</Text>
      </Text>
    );
  }
  return (
    <Box flexDirection="column">
      <Text>
        <Text color="cyan">?</Text> <Text bold>{message}</Text>
        {hint ? <Text dimColor>{hint}</Text> : null}
      </Text>
      {children}
    </Box>
  );
}

function Select<T>(
  { message, choices, finish }: {
    message: string;
    choices: Choice<T>[];
    finish: Finish<T>;
  },
) {
  const { answer, submit, cancel } = useAnswer(finish);
  const [index, setIndex] = useState(0);
  useInput((input, key) => {
    if (key.ctrl && input === "c") cancel();
    else if (key.upArrow) {
      setIndex((i) => (i + choices.length - 1) % choices.length);
    } else if (key.downArrow) setIndex((i) => (i + 1) % choices.length);
    else if (key.return) submit(choices[index].value, choices[index].label);
  }, { isActive: answer === undefined });
  return (
    <Frame message={message} answer={answer}>
      {choices.map((c, i) => (
        <Text key={i} color={i === index ? "cyan" : undefined}>
          {i === index ? "❯ " : "  "}
          {c.label}
          {c.hint ? <Text dimColor>{`   ${c.hint}`}</Text> : null}
        </Text>
      ))}
    </Frame>
  );
}

function MultiSelect<T>(
  { message, choices, finish }: {
    message: string;
    choices: Choice<T>[];
    finish: Finish<T[]>;
  },
) {
  const { answer, submit, cancel } = useAnswer(finish);
  const [index, setIndex] = useState(0);
  const [picked, setPicked] = useState(() => new Set(choices.keys()));
  useInput((input, key) => {
    if (key.ctrl && input === "c") cancel();
    else if (key.upArrow) {
      setIndex((i) => (i + choices.length - 1) % choices.length);
    } else if (key.downArrow) setIndex((i) => (i + 1) % choices.length);
    else if (input === " ") {
      setPicked((p) => {
        const next = new Set(p);
        if (!next.delete(index)) next.add(index);
        return next;
      });
    } else if (key.return) {
      const values = choices.filter((_, i) => picked.has(i));
      submit(values.map((c) => c.value), `${values.length} selected`);
    }
  }, { isActive: answer === undefined });
  return (
    <Frame message={message} answer={answer} hint="  (space toggles)">
      {choices.map((c, i) => (
        <Text key={i} color={i === index ? "cyan" : undefined}>
          {i === index ? "❯ " : "  "}
          {picked.has(i) ? "[x] " : "[ ] "}
          {c.label}
          {c.hint ? <Text dimColor>{`   ${c.hint}`}</Text> : null}
        </Text>
      ))}
    </Frame>
  );
}

function Confirm(
  { message, initial, finish }: {
    message: string;
    initial: boolean;
    finish: Finish<boolean>;
  },
) {
  const { answer, submit, cancel } = useAnswer(finish);
  useInput((input, key) => {
    // Typed-ahead input can arrive as "y\r" in one chunk.
    const typed = input.replace(/[\r\n]/g, "");
    if (key.ctrl && input === "c") cancel();
    else if (/^y/i.test(typed)) submit(true, "yes");
    else if (/^n/i.test(typed)) submit(false, "no");
    else if (key.return || /[\r\n]/.test(input)) {
      submit(initial, initial ? "yes" : "no");
    }
  }, { isActive: answer === undefined });
  return (
    <Frame
      message={message}
      answer={answer}
      hint={initial ? " (Y/n)" : " (y/N)"}
    />
  );
}

function TextInput(
  { message, mask, finish }: {
    message: string;
    mask: boolean;
    finish: Finish<string>;
  },
) {
  const { answer, submit, cancel } = useAnswer(finish);
  const [value, setValue] = useState("");
  const current = useRef("");
  const update = (next: string) => {
    current.current = next;
    setValue(next);
  };
  const done = (v: string) => {
    if (v) submit(v, mask ? "•".repeat(8) : v);
  };
  useInput((input, key) => {
    if (key.ctrl && input === "c") cancel();
    else if (key.return) done(current.current);
    else if (key.backspace || key.delete) update(current.current.slice(0, -1));
    else if (!key.ctrl && !key.meta && input) {
      // Pasted text can arrive together with the Enter that ends it.
      const end = input.search(/[\r\n]/);
      if (end < 0) update(current.current + input);
      else done(current.current + input.slice(0, end));
    }
  }, { isActive: answer === undefined });
  const shown = mask ? "•".repeat(value.length) : value;
  return (
    <Frame message={message} answer={answer}>
      <Text>{`  ${shown}`}</Text>
    </Frame>
  );
}

export const inkPrompter: Prompter = {
  interactive: true,
  select: (message, choices) =>
    ask((finish) => (
      <Select message={message} choices={choices} finish={finish} />
    )),
  multiSelect: (message, choices) =>
    ask((finish) => (
      <MultiSelect message={message} choices={choices} finish={finish} />
    )),
  confirm: (message, initial) =>
    ask((finish) => (
      <Confirm message={message} initial={initial} finish={finish} />
    )),
  text: (message) =>
    ask((finish) => (
      <TextInput message={message} mask={false} finish={finish} />
    )),
  secret: (message) =>
    ask((finish) => <TextInput message={message} mask finish={finish} />),
};
