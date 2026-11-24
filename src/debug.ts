/** Whether diagnostic logging is enabled for this invocation. */
export function debugEnabled(): boolean {
  const value = process.env.DH_DEBUG;
  return value !== undefined && value !== "" && value !== "0";
}
