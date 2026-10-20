# UI

This crate contains shared design-system foundations and UI components for the workspace. Platform crates provide composition roots and platform-specific dependencies.

```
ui/
├─ src/
│  ├─ lib.rs # The entrypoint for the ui crate
│  ├─ components/ # Shared shadcn-like primitives (tabs, buttons, cards, separators)
│  ├─ foundation.rs # Shared design-token stylesheet loader
│  ├─ tiles/ # Tile grammar prototype (not yet shippable)
├─ styles/ # Source Material, Dayframe, and tile CSS
├─ tailwind.css # Tailwind entrypoint importing the source styles
├─ assets/
│  └─ tailwind.css # Generated Tailwind output only
```

## Dependencies

Since this crate is shared between multiple platforms, it should not pull in any platform specific dependencies. For example, if you want to use the `web_sys` crate in the web build of your app, you should not add it to this crate. Instead, you should add platform specific dependencies to the [web](../web/Cargo.toml), [desktop](../desktop/Cargo.toml), or [mobile](../mobile/Cargo.toml) crates.
