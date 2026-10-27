// Structural sketch only. Executable consumption lives in dayframe_app, not this file.
// Capabilities are opaque authority; API contracts are separate typed dependencies.

declare const authority: unique symbol;
type Capability<T> = { readonly [authority]: T };
type InstanceId = string;
type ComponentRef = Capability<"component">;
type Route = { document: string; branch: string; tag: string; keyId: string };
type Selection = {
  documents: ReadonlySet<string>;
  branch: string;
  facets: { tags: ReadonlySet<string> } | { keys: readonly [string, string][] };
};
type Snapshot = {
  revision: number; // Mirror delivery revision, never a source cursor.
  facets: readonly {
    route: Route;
    value: "loading" | "absent" | { present: unknown };
  }[];
  pending: readonly string[];
  refreshing: boolean;
  revoked: boolean;
};
interface DocumentAccess {
  readonly token: Capability<"document-access">;
  document(id: string, branch: string): DocumentRef;
  watch(selection: Selection): DocumentWatch;
  mutate(
    route: Route,
    intent: { write: unknown } | { create: unknown } | "delete",
  ): string;
}
interface DocumentRef {
  facet<T>(
    tag: string,
    keyId: string,
  ): {
    watch: DocumentWatch;
    value(snapshot: Snapshot): T | undefined;
    write(value: T): string;
  };
}
interface DocumentWatch {
  current(): Snapshot;
  updates(): AsyncIterable<Snapshot>; // Atomic initial snapshot + bounded/coalesced updates.
  select(selection: Selection): void;
  refresh(): void;
  close(): void;
}
interface Contribution<T> {
  insert(value: T): { update(value: T): void; remove(): void };
}
type SettingsSection = { title: string; detail: ComponentRef };
type SettingsApi = Contribution<SettingsSection>;
type SampleContext = {
  instance: InstanceId;
  settings: SettingsApi; // Explicit binding, no generic service lookup.
  configDocument: DocumentRef;
};
type SettingsContext = {
  instance: InstanceId;
  sections: Capability<"settings-collection-owner">;
  roots: Contribution<{ title: string; content: ComponentRef }>;
};
type Host = {
  instances: Map<InstanceId, { effects: readonly unknown[] }>;
  dependencies: Map<InstanceId, ReadonlySet<InstanceId>>;
  collections: Map<string, unknown>; // Host storage; typed handles restrict operations.
  components: Map<ComponentRef, InstanceId>;
  documents: {
    retainedProjection: Map<string, unknown>;
    optimisticTransactions: Map<string, unknown>;
    authorizedMirrors: Map<InstanceId, readonly DocumentWatch[]>;
  };
};

export type { DocumentAccess, Host, SampleContext, SettingsContext };
