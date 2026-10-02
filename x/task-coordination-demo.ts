#!/usr/bin/env -S deno run

// Executable thought experiment for ADRs 010 and 011.
//
// This intentionally models only the protocol boundaries:
//   BigSync       durable store-and-forward
//   BigEphemeral  lossy router heartbeat discovery, no replay and no RPC
//   RoutingRpc    live point-to-point registration and allocation
//   Domain        authoritative obligation/settlement policy
//   DispatchRepo  one node's durable local attempts

type NodeId = "phone" | "laptop" | "desktop";
type PoolId = "image-description" | "agent-background";
type TaskId = string;
type Generation = string;
type Capability = "gpu" | "web" | "llm";

type TerminalFact =
  | { kind: "succeeded"; attemptId: string; result: string }
  | { kind: "cancelled"; reason: string };

class TaskTicket {
  readonly terminalFacts = new Map<string, TerminalFact>();
  active = true;

  constructor(
    readonly id: TaskId,
    readonly pool: PoolId,
    readonly domain: "triage" | "standalone-agent",
    readonly generation: Generation,
    readonly requiredCapabilities: ReadonlySet<Capability>,
    readonly requiredInput: string | undefined,
    readonly preferredNode: NodeId | undefined,
    readonly resultRetention: "external-settlement" | "ticket-authoritative",
    readonly notAfter: number | undefined,
  ) {}

  clone(): TaskTicket {
    const copy = new TaskTicket(
      this.id,
      this.pool,
      this.domain,
      this.generation,
      this.requiredCapabilities,
      this.requiredInput,
      this.preferredNode,
      this.resultRetention,
      this.notAfter,
    );
    copy.active = this.active;
    for (const [writer, fact] of this.terminalFacts) {
      copy.terminalFacts.set(writer, structuredClone(fact));
    }
    return copy;
  }

  merge(other: TaskTicket): void {
    if (
      JSON.stringify(this.declaration()) !== JSON.stringify(other.declaration())
    ) {
      throw new Error(`unequal declarations collided at ${this.id}`);
    }
    for (const [writer, fact] of other.terminalFacts) {
      this.terminalFacts.set(writer, structuredClone(fact));
    }
    this.active &&= other.active;
  }

  isTerminal(): boolean {
    return this.terminalFacts.size > 0;
  }

  private declaration(): unknown {
    return {
      id: this.id,
      pool: this.pool,
      domain: this.domain,
      generation: this.generation,
      requiredCapabilities: [...this.requiredCapabilities].sort(),
      requiredInput: this.requiredInput,
      preferredNode: this.preferredNode,
      resultRetention: this.resultRetention,
      notAfter: this.notAfter,
    };
  }
}

class RouterClaim {
  constructor(
    readonly pool: PoolId,
    readonly candidate: NodeId,
    readonly generation: number,
    readonly claimId: string,
  ) {}

  rank(): string {
    return `${
      this.generation.toString().padStart(8, "0")
    }/${this.candidate}/${this.claimId}`;
  }
}

class RouterSlot {
  readonly claims = new Map<NodeId, RouterClaim>();

  claim(pool: PoolId, candidate: NodeId): RouterClaim {
    const generation = Math.max(
      0,
      ...[...this.claims.values()].map((claim) => claim.generation),
    ) + 1;
    const claim = new RouterClaim(
      pool,
      candidate,
      generation,
      `${candidate}-${generation}`,
    );
    this.claims.set(candidate, claim);
    return claim;
  }

  winner(): RouterClaim | undefined {
    const maximal = Math.max(
      0,
      ...[...this.claims.values()].map((claim) => claim.generation),
    );
    return [...this.claims.values()]
      .filter((claim) => claim.generation === maximal)
      .sort((left, right) => left.rank().localeCompare(right.rank()))[0];
  }

  clone(): RouterSlot {
    const copy = new RouterSlot();
    for (const [candidate, claim] of this.claims) {
      copy.claims.set(candidate, claim);
    }
    return copy;
  }

  merge(other: RouterSlot): void {
    for (const [candidate, incoming] of other.claims) {
      const current = this.claims.get(candidate);
      if (current === undefined || incoming.generation > current.generation) {
        this.claims.set(candidate, incoming);
      }
    }
  }
}

class BigSyncReplica {
  readonly tasks = new Map<TaskId, TaskTicket>();
  readonly routers = new Map<PoolId, RouterSlot>();

  constructor(readonly name: string) {}

  putTask(task: TaskTicket): void {
    const current = this.tasks.get(task.id);
    if (current === undefined) this.tasks.set(task.id, task.clone());
    else current.merge(task);
  }

  routerSlot(pool: PoolId): RouterSlot {
    let slot = this.routers.get(pool);
    if (slot === undefined) {
      slot = new RouterSlot();
      this.routers.set(pool, slot);
    }
    return slot;
  }

  syncWith(other: BigSyncReplica): void {
    for (const task of this.tasks.values()) other.putTask(task);
    for (const task of other.tasks.values()) this.putTask(task);
    for (const [pool, slot] of this.routers) other.routerSlot(pool).merge(slot);
    for (const [pool, slot] of other.routers) this.routerSlot(pool).merge(slot);
  }
}

type RouterHeartbeat = {
  pool: PoolId;
  router: NodeId;
  claimGeneration: number;
  claimId: string;
  rpcAddress: string;
};

class BigEphemeral {
  private readonly subscribers = new Set<
    (heartbeat: RouterHeartbeat) => void
  >();

  subscribe(listener: (heartbeat: RouterHeartbeat) => void): void {
    this.subscribers.add(listener);
  }

  publish(heartbeat: RouterHeartbeat): void {
    console.log(
      `  ephemeral heartbeat ${heartbeat.pool}: router=${heartbeat.router}`,
    );
    for (const subscriber of this.subscribers) subscriber(heartbeat);
    // Deliberately retained nowhere. Late subscribers see nothing.
  }
}

type Classification =
  | "runnable"
  | "not-ready"
  | "coordination-incomplete"
  | "obsolete";

interface TaskDomain {
  classify(node: DaybookNode, task: TaskTicket, now: number): Classification;
  acceptSuccess(
    task: TaskTicket,
    fact: Extract<TerminalFact, { kind: "succeeded" }>,
  ): void;
  reconcile(node: DaybookNode, task: TaskTicket): void;
}

class TriageDomain implements TaskDomain {
  readonly desired = new Map<string, Generation>();
  readonly settled = new Map<string, Generation>();

  constructor(readonly slot: string) {}

  taskId(generation: Generation): TaskId {
    return `triage/${this.slot}/${generation}`;
  }

  desire(generation: Generation): void {
    this.desired.set(this.slot, generation);
  }

  classify(node: DaybookNode, task: TaskTicket): Classification {
    if (!this.desired.has(this.slot)) return "coordination-incomplete";
    if (this.settled.get(this.slot) === task.generation) return "obsolete";
    if (this.desired.get(this.slot) !== task.generation) return "obsolete";
    if (
      task.requiredInput !== undefined && !node.inputs.has(task.requiredInput)
    ) return "not-ready";
    return "runnable";
  }

  acceptSuccess(
    task: TaskTicket,
    _fact: Extract<TerminalFact, { kind: "succeeded" }>,
  ): void {
    if (this.desired.get(this.slot) === task.generation) {
      this.settled.set(this.slot, task.generation);
    }
  }

  reconcile(node: DaybookNode, task: TaskTicket): void {
    if (this.classify(node, task) === "obsolete") {
      node.cancel(task.id);
      task.active = false;
    }
  }
}

class StandaloneAgentDomain implements TaskDomain {
  classify(node: DaybookNode, task: TaskTicket, now: number): Classification {
    if (task.isTerminal()) return "obsolete";
    if (task.notAfter !== undefined && now >= task.notAfter) return "obsolete";
    if (
      task.requiredInput !== undefined && !node.inputs.has(task.requiredInput)
    ) return "not-ready";
    return "runnable";
  }

  acceptSuccess(
    _task: TaskTicket,
    _fact: Extract<TerminalFact, { kind: "succeeded" }>,
  ): void {
    // There is no external AgentRun. The TaskTicket remains the result record.
  }

  reconcile(node: DaybookNode, task: TaskTicket): void {
    if (this.classify(node, task, Date.now()) === "obsolete") {
      node.cancel(task.id);
      task.active = false; // Leaves active scheduling, but remains archived.
    }
  }
}

class DispatchAttempt {
  state: "running" | "cancel-requested" | "complete" = "running";

  constructor(readonly id: string, readonly task: TaskTicket) {}
}

class DispatchRepo {
  readonly attempts = new Map<TaskId, DispatchAttempt>();

  constructor(readonly node: NodeId) {}

  start(task: TaskTicket): DispatchAttempt {
    if (this.attempts.has(task.id)) {
      throw new Error(`${this.node} already attempted ${task.id}`);
    }
    const attempt = new DispatchAttempt(`${this.node}/${task.id}`, task);
    this.attempts.set(task.id, attempt);
    return attempt;
  }
}

type ExecutorRegistration = {
  node: DaybookNode;
  capabilities: ReadonlySet<Capability>;
  activeAttempts: ReadonlySet<TaskId>;
};

class RoutingRpc {
  private readonly routers = new Map<string, PoolRouter>();

  listen(address: string, router: PoolRouter): void {
    this.routers.set(address, router);
  }

  connect(address: string, registration: ExecutorRegistration): void {
    const router = this.routers.get(address);
    if (router === undefined) throw new Error(`unreachable router ${address}`);
    router.register(registration);
  }
}

class PoolRouter {
  private readonly executors = new Map<NodeId, ExecutorRegistration>();

  constructor(readonly pool: PoolId, readonly node: DaybookNode) {}

  register(registration: ExecutorRegistration): void {
    this.executors.set(registration.node.id, registration);
    console.log(
      `  rpc register ${registration.node.id} -> router ${this.node.id}`,
    );
  }

  route(task: TaskTicket, domain: TaskDomain, now: number): void {
    if (!task.active || task.isTerminal()) return;
    const registrations = [...this.executors.values()].sort((left, right) => {
      const leftPreferred = left.node.id === task.preferredNode ? 0 : 1;
      const rightPreferred = right.node.id === task.preferredNode ? 0 : 1;
      return leftPreferred - rightPreferred;
    });
    for (const registration of registrations) {
      const capable = [...task.requiredCapabilities].every((capability) =>
        registration.capabilities.has(capability)
      );
      if (!capable || registration.activeAttempts.has(task.id)) continue;
      if (domain.classify(registration.node, task, now) !== "runnable") {
        continue;
      }
      registration.node.accept(task);
      return;
    }
    console.log(`  router ${this.node.id}: no ready executor for ${task.id}`);
  }
}

class DaybookNode {
  readonly bigSync: BigSyncReplica;
  readonly dispatch: DispatchRepo;
  readonly inputs = new Set<string>();
  readonly domains = new Map<TaskTicket["domain"], TaskDomain>();
  private router?: PoolRouter;

  constructor(
    readonly id: NodeId,
    readonly capabilities: ReadonlySet<Capability>,
    private readonly ephemeral: BigEphemeral,
    private readonly rpc: RoutingRpc,
  ) {
    this.bigSync = new BigSyncReplica(id);
    this.dispatch = new DispatchRepo(id);
    this.ephemeral.subscribe((heartbeat) => this.observeRouter(heartbeat));
  }

  installDomain(name: TaskTicket["domain"], domain: TaskDomain): void {
    this.domains.set(name, domain);
  }

  claimRouter(pool: PoolId): PoolRouter {
    const claim = this.bigSync.routerSlot(pool).claim(pool, this.id);
    const winner = this.bigSync.routerSlot(pool).winner();
    if (winner !== claim) throw new Error(`${this.id} lost router election`);
    this.router = new PoolRouter(pool, this);
    const rpcAddress = `iroh://${this.id}/${pool}`;
    this.rpc.listen(rpcAddress, this.router);
    this.ephemeral.publish({
      pool,
      router: this.id,
      claimGeneration: claim.generation,
      claimId: claim.claimId,
      rpcAddress,
    });
    return this.router;
  }

  observeRouter(heartbeat: RouterHeartbeat): void {
    if (!this.capabilities.size) return;
    this.rpc.connect(heartbeat.rpcAddress, {
      node: this,
      capabilities: this.capabilities,
      activeAttempts: new Set(
        [...this.dispatch.attempts.values()]
          .filter((attempt) => attempt.state === "running")
          .map((attempt) => attempt.task.id),
      ),
    });
  }

  accept(task: TaskTicket): void {
    const domain = this.domains.get(task.domain);
    if (
      domain === undefined ||
      domain.classify(this, task, Date.now()) !== "runnable"
    ) return;
    this.dispatch.start(task);
    console.log(`  ${this.id} starts ${task.id}`);
  }

  complete(taskId: TaskId, result: string): void {
    const attempt = this.dispatch.attempts.get(taskId);
    if (attempt === undefined) {
      throw new Error(`${this.id} is not running ${taskId}`);
    }
    attempt.state = "complete";
    const fact = { kind: "succeeded", attemptId: attempt.id, result } as const;
    attempt.task.terminalFacts.set(this.id, fact);
    const domain = this.domains.get(attempt.task.domain);
    if (domain === undefined) {
      throw new Error(`missing domain ${attempt.task.domain}`);
    }
    domain.acceptSuccess(attempt.task, fact);
    domain.reconcile(this, attempt.task);
  }

  cancel(taskId: TaskId): void {
    const attempt = this.dispatch.attempts.get(taskId);
    if (attempt?.state === "running") attempt.state = "cancel-requested";
  }
}

class Demo {
  readonly ephemeral = new BigEphemeral();
  readonly rpc = new RoutingRpc();
  readonly relay = new BigSyncReplica("encrypted-relay");
  readonly triage = new TriageDomain("photo-7/describe-image");
  readonly agent = new StandaloneAgentDomain();
  readonly phone = new DaybookNode(
    "phone",
    new Set(),
    this.ephemeral,
    this.rpc,
  );
  readonly laptop = new DaybookNode(
    "laptop",
    new Set(["web", "llm"]),
    this.ephemeral,
    this.rpc,
  );
  readonly desktop = new DaybookNode(
    "desktop",
    new Set(["gpu", "web", "llm"]),
    this.ephemeral,
    this.rpc,
  );

  constructor() {
    for (const node of [this.phone, this.laptop, this.desktop]) {
      node.installDomain("triage", this.triage);
      node.installDomain("standalone-agent", this.agent);
    }
    this.desktop.inputs.add("photo-7");
  }

  run(): void {
    this.step("phone publishes triage work; relay carries it", () => {
      this.triage.desire("G1");
      this.phone.bigSync.putTask(
        new TaskTicket(
          this.triage.taskId("G1"),
          "image-description",
          "triage",
          "G1",
          new Set(["gpu"]),
          "photo-7",
          "phone",
          "external-settlement",
          undefined,
        ),
      );
      this.phone.bigSync.syncWith(this.relay);
      this.desktop.bigSync.syncWith(this.relay);
    });

    let imageRouter: PoolRouter;
    this.step(
      "desktop wins durable election and advertises only a heartbeat",
      () => {
        imageRouter = this.desktop.claimRouter("image-description");
      },
    );

    this.step(
      "router allocates over RPC; executor settles triage and prunes",
      () => {
        const task = this.desktop.bigSync.tasks.get(this.triage.taskId("G1"));
        if (task === undefined) throw new Error("missing triage task");
        imageRouter.route(task, this.triage, Date.now());
        this.desktop.complete(task.id, "a lake at sunset");
      },
    );

    this.step("stale task resurrects but settlement makes it inert", () => {
      this.phone.bigSync.syncWith(this.desktop.bigSync);
      const stale = this.phone.bigSync.tasks.get(this.triage.taskId("G1"));
      if (stale === undefined) throw new Error("missing stale task");
      stale.active = true; // Model an old partition reintroducing membership.
      this.triage.reconcile(this.desktop, stale);
      if (stale.active) throw new Error("settled task became active");
    });

    let agentRouter: PoolRouter;
    this.step("standalone agent task has no external settlement", () => {
      const task = new TaskTicket(
        "agent/research-trains/1",
        "agent-background",
        "standalone-agent",
        "occurrence-1",
        new Set(["web", "llm"]),
        undefined,
        "laptop",
        "ticket-authoritative",
        Date.now() + 86_400_000,
      );
      this.phone.bigSync.putTask(task);
      this.phone.bigSync.syncWith(this.relay);
      this.laptop.bigSync.syncWith(this.relay);
      agentRouter = this.laptop.claimRouter("agent-background");
      agentRouter.route(
        this.laptop.bigSync.tasks.get(task.id)!,
        this.agent,
        Date.now(),
      );
      this.laptop.complete(task.id, "report://train-options");
    });

    this.step(
      "agent result leaves active scheduling but remains durable",
      () => {
        const task = this.laptop.bigSync.tasks.get("agent/research-trains/1");
        if (task === undefined || !task.isTerminal() || task.active) {
          throw new Error("agent terminal retention invariant failed");
        }
        this.laptop.bigSync.syncWith(this.relay);
      },
    );
  }

  step(title: string, action: () => void): void {
    console.log(`\n=== ${title} ===`);
    action();
    this.show();
  }

  show(): void {
    console.log(
      `  triage desired=${
        this.triage.desired.get(this.triage.slot) ?? "none"
      } settled=${this.triage.settled.get(this.triage.slot) ?? "none"}`,
    );
    for (
      const replica of [
        this.phone.bigSync,
        this.laptop.bigSync,
        this.desktop.bigSync,
        this.relay,
      ]
    ) {
      const tasks = [...replica.tasks.values()].map((task) =>
        `${task.id}:${task.active ? "active" : "inactive"}${
          task.isTerminal() ? "/terminal" : ""
        }`
      );
      console.log(`  ${replica.name}: ${tasks.join(", ") || "empty"}`);
    }
  }
}

new Demo().run();
