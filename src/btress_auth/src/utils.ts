export type OrRetOf<T> = T extends () => infer Inner ? Inner : T;

/**
 * This tries to emulate a rust `match` statement but in a typesafe
 * way. This is a WIP function.
 * ```ts
 * const pick: 2 = switchMap(
 *   "hello",
 *   {
 *     hey: () => 1,
 *     hello: () => 2,
 *     hi: 3,
 *     holla: 4,
 *   },
 * );
 * ```
 */
export function switchMap<
  const All extends Record<string | number | symbol, unknown>,
  const K extends string | number | symbol = string,
>(val: K, branches: All): K extends keyof All ? OrRetOf<All[K]> : undefined {
  const branch = branches[val];
  return typeof branch === "function"
    ? branch()
    : (branch as K extends keyof All ? OrRetOf<All[K]> : undefined);
}
