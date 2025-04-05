import { useCallback, useEffect, useMemo, useRef, useState } from "react";

export function usePreparedCallbacks<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  T extends Record<string, (...all: any[]) => Promise<unknown>>,
>(actions: T) {
  const [fetchingMap, setFetchingMap] = useState(
    Object.fromEntries(
      Object.keys(actions).map((key) => [key, false]),
    ) as Record<keyof T, boolean>,
  );
  const newActions = {} as T;

  for (const key of useMemo(() => Object.keys(actions).sort(), [actions])) {
    const fn = actions[key];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    newActions[key as keyof T] = (async (...args: any[]) => {
      const logger = console;
      // const logger = loggerFor("preparedCallback." + key);
      console.info({ key, args }, "invoked");
      setFetchingMap({
        ...fetchingMap,
        [key]: true,
      });
      try {
        await fn(...args);
      } catch (err: unknown) {
        logger.error(
          { name: key, err: err?.toString?.() ?? err },
          "button action error",
        );
        const errMessage = err instanceof Object ? " " + err.toString() : "";
        // notifications.show({
        //   title: "Sorry, looks like that didn't work.",
        //   message: "We have an error!" + errMessage,
        // });
      } finally {
        setFetchingMap({
          ...fetchingMap,
          [key]: false,
        });
      }
    }) as T[keyof T];
  }
  return [newActions, fetchingMap, setFetchingMap] as const;
}

export function useAsyncError() {
  const [, setError] = useState();
  return useCallback(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (error: any) => {
      setError(() => {
        throw error;
      });
    },
    [setError],
  );
}

// export function useDebouncedHandler<
//   Args extends [BaseSyntheticEvent<unknown>, ...unknown[]],
//   F extends (...args: Args) => unknown,
// >(
//   props: {
//     callback: F;
//     // eslint-disable-next-line @typescript-eslint/no-explicit-any
//     onError?: (err: any) => any;
//   }
// ) {
//   const debouncedCallback = useDebouncedCallback<Args, F>({
//     ...props,
//   })
//   return (...args: Args) => {
//     args[0].persist();
//     return debouncedCallback(...args);
//   };
// }
//
// export function useDebouncedCallback<
//   Args extends unknown[],
//   F extends (...args: Args) => unknown,
// >(
//   props: {
//     callback: F;
//     // eslint-disable-next-line @typescript-eslint/no-explicit-any
//     onError?: (err: any) => any;
//   }) {
//   const onUpdate = useLatestCallback(props.callback);
//   // debouncedCallback will be stable across renderes
//   // since it's only tracking a ref
//   const debouncedCallback = useMemo(() =>
//     debounce(
//       (...args: Args) => onUpdate(...args),
//       250,
//     ),
//     [onUpdate]
//   )
//
//   const throwError = useAsyncError()
//   const onError = useLatestCallback(props.onError ?? throwError);
//
//   useEffect(() => {
//     return () => {
//       try {
//         const res = debouncedCallback.flush();
//         if (res instanceof Promise) {
//           res?.catch(onError);
//         }
//       } catch (err) {
//         onError(err)
//       }
//     }
//   }, [debouncedCallback, onError]);
//   return debouncedCallback;
// }

export function useLatestRef<T>(value: T) {
  const ref = useRef(value);
  useEffect(() => {
    ref.current = value;
  }, [value]);
  return ref;
}

export function useLatestCallback<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Args extends any[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  F extends (...args: Args) => any,
>(callback: F): F {
  const callbackRef = useRef(callback);

  // Update the ref with the latest callback on every render.
  useEffect(() => {
    callbackRef.current = callback;
  }, [callback]);

  // Return a stable function that always calls the latest callback.
  return useCallback((...args: Parameters<F>) => {
    return callbackRef.current(...args);
  }, []) as F;
}
