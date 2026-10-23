# dayframe_dx

Dioxus-native entrypoints and shared design-system work for Dayframe.

## Packages

- `ui` — shared foundations, Material-derived tokens, and UI prototypes.
- `showcase` — isolated native design-system inspection app.
- `desktop` — desktop composition root and renderer experiments.
- `mobile` — Android composition root.
- `web` — web composition root.

## Design-system showcase

```sh
target/dioxus-cli/bin/dx serve -p showcase --platform desktop --renderer native
```

The showcase is deliberately disconnected from the real core. It renders the Material palettes and schemes from the checked-in JSON fixture and exercises the shared token contract.

## Android app

```sh
cd mobile
../../../target/dioxus-cli/bin/dx serve \
  --platform android \
  --target aarch64-linux-android \
  --client-target aarch64-linux-android \
  --device \
  --hot-patch false \
  --renderer native
```
