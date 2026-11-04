module wflow_authority {
  const KEYS: Set[str]
  const WORKERS: Set[int]
  const MESSAGE_IDS: Set[str]

  type Lease = {
    partition_incarnation: int,
    authority_epoch: int,
    job_incarnation: int,
    generation: int,
    worker: int,
    position: int,
  }

  type Step = {
    id: int,
    kind: str,
    // The boundary name the workflow chose; "" models Rust's `None`, so a
    // replay that renames a step diverges exactly as the authority rejects it.
    name: str,
    attempt: int,
    value: str,
    lease: Lease,
    position: int,
  }

  type Terminal = {
    value: str,
    lease: Lease,
    position: int,
  }

  // A durable wait. `timer` selects the kind: a timer wait carries the
  // `fire_at` deadline the worker computed, a message wait leaves it 0.
  type Wait = {
    step: int,
    timer: bool,
    fire_at: int,
    lease: Lease,
    position: int,
  }

  // A durable retry the job is parked on after a transient failure.
  type RetryWait = {
    fire_at: int,
    error: str,
    position: int,
  }

  // The classified failure that settled a job which cannot make progress.
  // `failure` names the Rust `JobFailure` variant; `restarts` carries the
  // count for "restarts-exhausted" and is 0 for every other failure.
  type Fault = {
    failure: str,
    restarts: int,
    position: int,
  }

  // How far a job had got when its current run was granted. `waiting` is the
  // step number of the held wait, or -1 for Rust's `None`.
  type Progress = {
    steps: int,
    waiting: int,
  }

  type Message = {
    id: str,
    payload: str,
    position: int,
  }

  type Job = {
    key: str,
    args: str,
    incarnation: int,
    submission_position: int,
    steps: List[Step],
    last_generation: int,
    leased: bool,
    lease: Lease,
    waiting: bool,
    wait: Wait,
    inbox: List[Message],
    delivered: int,
    has_terminal: bool,
    terminal: Terminal,
    retrying: bool,
    retry: RetryWait,
    retry_attempts: int,
    restarts: int,
    has_progress: bool,
    progress: Progress,
    has_fault: bool,
    fault: Fault,
  }

  type Intent = {
    kind: str,
    key: str,
    lease: Lease,
    replay: List[Step],
    step: Step,
    wait: Wait,
    terminal: Terminal,
    fault: Fault,
  }

  type Outcome = {
    kind: str,
    position: int,
    disposition: str,
    reason: str,
  }

  type State = {
    partition_incarnation: int,
    authority_epoch: int,
    revision: int,
    durable_len: int,
    next_job_incarnation: int,
    jobs: List[Job],
    intents: List[Intent],
    outcome: Outcome,
  }

  // `resolution` is only meaningful for "wait_resolved" records: "timer",
  // "queued" (the oldest undelivered inbox message), or "delivered" (`message`
  // arrived while the job waited for one and is accepted in the same record).
  type Record = {
    kind: str,
    key: str,
    args: str,
    incarnation: int,
    lease: Lease,
    step: Step,
    wait: Wait,
    resolution: str,
    message: Message,
    terminal: Terminal,
    restarts: int,
    progress: Progress,
    retry: RetryWait,
    fault: Fault,
  }

  var state: State
  var durable: List[Record]

  // The fixed deadline every sampled timer wait begins with. The authority
  // never reads a clock, so its value only has to round-trip.
  pure val TIMER_FIRE_AT = 5

  // The retry budget every sampled submission carries, and the fixed delay
  // its policy schedules. Small on purpose: one transient failure retries
  // once, one progress-free re-run restarts once, and the next faults.
  pure val RETRY_DELAY = 3
  pure val MAX_RETRIES = 1
  pure val MAX_RESTARTS = 1

  // The boundary names sampled steps and waits choose from. "" is Rust's
  // `None`; the divergent-replay actions invert along this two-element set.
  pure val STEP_NAMES = Set("", "named")

  def stepName(name: str): str = if (name == "") "named" else ""

  def emptyLease: Lease = {
    partition_incarnation: 0,
    authority_epoch: 0,
    job_incarnation: 0,
    generation: 0,
    worker: 0,
    position: 0,
  }

  def emptyStep: Step = {
    id: 0,
    kind: "effect",
    name: "",
    attempt: 0,
    value: "",
    lease: emptyLease,
    position: 0,
  }

  def emptyTerminal: Terminal = {
    value: "",
    lease: emptyLease,
    position: 0,
  }

  def emptyWait: Wait = {
    step: 0,
    timer: false,
    fire_at: 0,
    lease: emptyLease,
    position: 0,
  }

  def emptyRetryWait: RetryWait = {
    fire_at: 0,
    error: "",
    position: 0,
  }

  def emptyFault: Fault = {
    failure: "",
    restarts: 0,
    position: 0,
  }

  def emptyProgress: Progress = {
    steps: 0,
    waiting: -1,
  }

  def emptyMessage: Message = {
    id: "",
    payload: "",
    position: 0,
  }

  def emptyJob: Job = {
    key: "",
    args: "",
    incarnation: 0,
    submission_position: 0,
    steps: [],
    last_generation: 0,
    leased: false,
    lease: emptyLease,
    waiting: false,
    wait: emptyWait,
    inbox: [],
    delivered: 0,
    has_terminal: false,
    terminal: emptyTerminal,
    retrying: false,
    retry: emptyRetryWait,
    retry_attempts: 0,
    restarts: 0,
    has_progress: false,
    progress: emptyProgress,
    has_fault: false,
    fault: emptyFault,
  }

  def emptyIntent: Intent = {
    kind: "",
    key: "",
    lease: emptyLease,
    replay: [],
    step: emptyStep,
    wait: emptyWait,
    terminal: emptyTerminal,
    fault: emptyFault,
  }

  def emptyRecord: Record = {
    kind: "",
    key: "",
    args: "",
    incarnation: 0,
    lease: emptyLease,
    step: emptyStep,
    wait: emptyWait,
    resolution: "",
    message: emptyMessage,
    terminal: emptyTerminal,
    restarts: 0,
    progress: emptyProgress,
    retry: emptyRetryWait,
    fault: emptyFault,
  }

  def outcome(kind: str, position: int, disposition: str, reason: str): Outcome = {
    kind: kind,
    position: position,
    disposition: disposition,
    reason: reason,
  }

  def initial: State = {
    partition_incarnation: 7,
    authority_epoch: 1,
    revision: 0,
    durable_len: 0,
    next_job_incarnation: 0,
    jobs: [],
    intents: [],
    outcome: outcome("init", 0, "", ""),
  }

  def findJob(jobs: List[Job], key: str): Job = jobs.foldl(emptyJob, (found, job) => if (job.key == key) job else found)

  def hasJob(jobs: List[Job], key: str): bool = findJob(jobs, key).key == key

  def replaceJob(jobs: List[Job], replacement: Job): List[Job] = jobs.foldl([], (result, job) => result.append(if (job.key == replacement.key) replacement else job))

  def clearLeases(jobs: List[Job]): List[Job] = jobs.foldl([], (result, job) => result.append({ ...job, leased: false, lease: emptyLease }))

  def findMessage(inbox: List[Message], id: str): Message = inbox.foldl(emptyMessage, (found, message) => if (message.id == id) message else found)

  def hasMessage(inbox: List[Message], id: str): bool = findMessage(inbox, id).id == id

  def executeIntent(key: str, lease: Lease, replay: List[Step]): Intent = {
    ...emptyIntent,
    kind: "execute",
    key: key,
    lease: lease,
    replay: replay,
  }

  def continueIntent(key: str, step: Step): Intent = {
    ...emptyIntent,
    kind: "continue",
    key: key,
    step: step,
  }

  def revokeIntent(key: str, lease: Lease): Intent = {
    ...emptyIntent,
    kind: "revoke",
    key: key,
    lease: lease,
  }

  def armIntent(key: str, wait: Wait): Intent = {
    ...emptyIntent,
    kind: "arm",
    key: key,
    wait: wait,
  }

  def terminalIntent(key: str, terminal: Terminal): Intent = {
    ...emptyIntent,
    kind: "terminal",
    key: key,
    terminal: terminal,
  }

  def faultIntent(key: str, fault: Fault): Intent = {
    ...emptyIntent,
    kind: "fault",
    key: key,
    fault: fault,
  }

  def isSettled(job: Job): bool = job.has_terminal or job.has_fault

  // The disposition a duplicate acknowledgement reports for the job's current
  // state, mirroring Rust's `disposition`.
  def disposition(job: Job): str =
    if (isSettled(job)) "completed"
    else if (job.leased and not(job.waiting) and not(job.retrying)) "continue"
    else "park"

  // How far the job is right now, mirroring Rust's `Job::progress`.
  def jobProgress(job: Job): Progress = {
    steps: job.steps.length(),
    waiting: if (job.waiting) job.wait.step else -1,
  }

  // The restart count a grant would record: a due-retry re-run keeps the
  // count, a re-run from unchanged progress (crash or lost lease) costs one,
  // and any progress since the last run resets it. Shared by the grant
  // actions and `applyToJob`, mirroring Rust's `restarts_after`.
  def restartsAfter(job: Job): int =
    if (job.retrying) job.restarts
    else if (job.has_progress and job.progress == jobProgress(job)) job.restarts + 1
    else 0

  // Only a leased invocation can be parked on a wait; an unleased job picks
  // the resolved step up from its replay when it is next granted.
  def resolvedIntents(job: Job, resolved: Step): List[Intent] =
    if (job.leased) [continueIntent(job.key, resolved)] else []

  def applyToJob(job: Job, record: Record): Job =
    if (record.kind == "grant") {
      ...job,
      last_generation: record.lease.generation,
      leased: true,
      lease: record.lease,
      restarts: record.restarts,
      has_progress: true,
      progress: record.progress,
      retrying: false,
      retry: emptyRetryWait,
    }
    else if (record.kind == "step") {
      ...job,
      steps: job.steps.append(record.step),
      retry_attempts: 0,
    }
    else if (record.kind == "complete") {
      ...emptyJob,
      key: job.key,
      args: job.args,
      incarnation: job.incarnation,
      submission_position: job.submission_position,
      has_terminal: true,
      terminal: record.terminal,
    }
    else if (record.kind == "revoke") {
      ...job,
      leased: false,
      lease: emptyLease,
    }
    else if (record.kind == "wait_started") {
      ...job,
      waiting: true,
      wait: record.wait,
    }
    else if (record.kind == "wait_resolved") {
      ...job,
      steps: job.steps.append(record.step),
      waiting: false,
      wait: emptyWait,
      retry_attempts: 0,
      inbox: if (record.resolution == "delivered") job.inbox.append(record.message) else job.inbox,
      delivered: if (record.resolution == "timer") job.delivered else job.delivered + 1,
    }
    else if (record.kind == "retry_scheduled") {
      ...job,
      leased: false,
      lease: emptyLease,
      retrying: true,
      retry: record.retry,
      retry_attempts: job.retry_attempts + 1,
    }
    else if (record.kind == "fault") {
      ...emptyJob,
      key: job.key,
      args: job.args,
      incarnation: job.incarnation,
      submission_position: job.submission_position,
      has_fault: true,
      fault: record.fault,
    }
    else {
      ...job,
      inbox: job.inbox.append(record.message),
    }

  def applyRecord(before: State, record: Record): State = {
    val position = before.revision + 1
    val applied = {
      ...before,
      revision: position,
      durable_len: position,
      intents: [],
    }
    if (record.kind == "submit") {
      ...applied,
      next_job_incarnation: record.incarnation,
      jobs: before.jobs.append({
        ...emptyJob,
        key: record.key,
        args: record.args,
        incarnation: record.incarnation,
        submission_position: position,
      }),
    }
    else {
      ...applied,
      jobs: before.jobs.replaceJob(before.jobs.findJob(record.key).applyToJob(record)),
    }
  }

  action commit(before: State, record: Record, intents: List[Intent], accepted: Outcome): bool = all {
    durable' = durable.append(record),
    state' = {
      ...before.applyRecord(record),
      intents: intents,
      outcome: accepted,
    },
  }

  action quiet(before: State, observed: Outcome): bool = all {
    durable' = durable,
    state' = { ...before, intents: [], outcome: observed },
  }

  action init = all {
    durable' = [],
    state' = initial,
  }

  action submit = {
    nondet key = KEYS.oneOf()
    all {
        not(state.jobs.hasJob(key)),
        val incarnation = state.next_job_incarnation + 1
        commit(
          state,
          { ...emptyRecord, kind: "submit", key: key, args: key, incarnation: incarnation },
          [],
          outcome("accepted", state.revision + 1, "admitted", ""),
        )
      }
  }

  action duplicateSubmit = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        quiet(state, outcome("duplicate", job.submission_position, disposition(job), ""))
      }
  }

  // The grant path proper, shared by `grant` and `grantDueRetry` (mirrors
  // Rust's `prepare_grant` after the settled/retry-pending/lease checks).
  // The restart budget is charged here: a re-run that starts without new
  // progress since the previous run costs one restart, and exceeding the
  // budget settles the job with a classified fault instead of leasing it.
  action grantRun(key: str, job: Job, worker: int): bool = {
    val restarts = restartsAfter(job)
    val lease = {
      partition_incarnation: state.partition_incarnation,
      authority_epoch: state.authority_epoch,
      job_incarnation: job.incarnation,
      generation: job.last_generation + 1,
      worker: worker,
      position: state.revision + 1,
    }
    if (restarts > MAX_RESTARTS) {
      val fault = { failure: "restarts-exhausted", restarts: restarts, position: state.revision + 1 }
      commit(
        state,
        { ...emptyRecord, kind: "fault", key: key, incarnation: job.incarnation, fault: fault },
        [faultIntent(key, fault)],
        outcome("accepted", state.revision + 1, "completed", ""),
      )
    } else
      commit(
        state,
        { ...emptyRecord, kind: "grant", key: key, lease: lease, restarts: restarts, progress: jobProgress(job) },
        [executeIntent(key, lease, job.steps)],
        outcome("accepted", state.revision + 1, "continue", ""),
      )
  }

  action grant = {
    nondet key = KEYS.oneOf()
    nondet worker = WORKERS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        // The host clock the driver passes with this grant is 0: a job parked
        // on a retry with a positive deadline is not due yet.
        if (job.retrying)
          quiet(state, outcome("rejected", 0, "", "retry-pending"))
        else all {
          not(isSettled(job)),
          not(job.leased),
          grantRun(key, job, worker),
        }
      }
  }

  // The host fires a due retry by re-granting with its clock at the deadline.
  // The due re-run is paid from the retry budget, so it keeps the restart count.
  action grantDueRetry = {
    nondet key = KEYS.oneOf()
    nondet worker = WORKERS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.retrying,
          not(isSettled(job)),
          not(job.leased),
          grantRun(key, job, worker),
        }
      }
  }

  // The leased invocation failed transiently; the authority releases the lease
  // and parks the job on a durable retry computed from its policy.
  action retry = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          not(job.waiting),
          job.retry_attempts < MAX_RETRIES,
          val retryWait = {
            fire_at: RETRY_DELAY,
            error: "transient",
            position: state.revision + 1,
          }
          commit(
            state,
            { ...emptyRecord, kind: "retry_scheduled", key: key, lease: job.lease, retry: retryWait },
            [],
            outcome("accepted", state.revision + 1, "park", ""),
          )
        }
      }
  }

  // Exhaustion rejects without mutation or publication. The lease remains
  // current so the runtime can settle through the normal completion path.
  action retryExhausted = {
    nondet key = KEYS.oneOf()
    all {
      state.jobs.hasJob(key),
      val job = state.jobs.findJob(key)
      all {
        job.leased,
        not(job.waiting),
        job.retry_attempts >= MAX_RETRIES,
        quiet(state, outcome("rejected", 0, "", "retries-exhausted")),
      }
    }
  }

  // A failure reported while durably suspended is an ordering violation:
  // the wait owns resumption.
  action retryWhileWaiting = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.waiting,
          quiet(state, outcome("rejected", 0, "", "out-of-order"))
        }
      }
  }

  // Cancellation atomically removes the active job, fences its lease and
  // publishes a permanent fault receipt before host-side teardown.
  action cancel = {
    nondet key = KEYS.oneOf()
    all {
      state.jobs.hasJob(key),
      val job = state.jobs.findJob(key)
      all {
        not(isSettled(job)),
        val fault = { failure: "cancelled", restarts: 0, position: state.revision + 1 }
        commit(
          state,
          { ...emptyRecord, kind: "fault", key: key, incarnation: job.incarnation, fault: fault },
          [faultIntent(key, fault)],
          outcome("accepted", state.revision + 1, "completed", ""),
        ),
      }
    }
  }

  // A later cancel of a cancelled receipt is rejected at the permanent
  // terminal boundary; cancellation is not a duplicate command.
  action duplicateCancel = {
    nondet key = KEYS.oneOf()
    all {
      state.jobs.hasJob(key),
      val job = state.jobs.findJob(key)
      all {
        job.has_fault,
        job.fault.failure == "cancelled",
        quiet(state, outcome("rejected", 0, "", "already-terminal")),
      }
    }
  }

  // Every other terminal receipt has the same permanent cancellation boundary.
  action cancelSettled = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          isSettled(job),
          not(job.has_fault and job.fault.failure == "cancelled"),
          quiet(state, outcome("rejected", 0, "", "already-terminal")),
        }
      }
  }

  // The effect record a lease holder commits at the job's next step.
  def nextStep(job: Job, kind: str, name: str, value: str, position: int): Step = {
    id: job.steps.length(),
    kind: kind,
    name: name,
    attempt: 0,
    value: value,
    lease: job.lease,
    position: position,
  }

  // The leased invocation commits an effect at its next step.
  action commitStep = {
    nondet key = KEYS.oneOf()
    nondet name = STEP_NAMES.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          not(job.waiting),
          val committed = nextStep(job, "effect", name, "value", state.revision + 1)
          commit(
            state,
            { ...emptyRecord, kind: "step", key: key, step: committed },
            [continueIntent(key, committed)],
            outcome("accepted", state.revision + 1, "continue", ""),
          )
        }
      }
  }

  // Only effects can be duplicated through CommitStep. Sleep and receive
  // records belong to the wait protocol and therefore diverge here.
  action duplicateStep = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.steps.length() > 0,
          val committed = job.steps[job.steps.length() - 1]
          if (committed.kind == "effect")
            quiet(state, outcome("duplicate", committed.position, "park", ""))
          else if (job.leased)
            quiet(state, outcome("rejected", 0, "", "nondeterministic"))
          else
            quiet(state, outcome("rejected", 0, "", "stale-lease"))
        }
      }
  }

  action staleResult = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          quiet(state, outcome("rejected", 0, "", "stale-lease"))
        }
      }
  }

  // The last committed step is replayed under an inverted boundary name: with
  // a current lease that is divergent replay, without one the replayer learns
  // its lease is gone first (mirrors `prepare_step`'s existing-step branch,
  // where the lease check runs before the kind/name comparison). Restricted
  // to non-sleep steps: the model's waits are unnamed, so no durable sleep
  // step ever carries a name to invert.
  action renamedStep = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.steps.length() > 0,
          val last = job.steps[job.steps.length() - 1]
          all {
            last.kind != "sleep",
            if (job.leased)
              quiet(state, outcome("rejected", 0, "", "nondeterministic"))
            else
              quiet(state, outcome("rejected", 0, "", "stale-lease"))
          }
        }
      }
  }

  // The last committed step is re-issued as a wait with an inverted name: an
  // effect step diverges on kind, a recv step on name (`prepare_wait`
  // compares the resolved step's kind and name before anything else), and
  // without a current lease the replayer learns its lease is gone first.
  action renamedWait = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.steps.length() > 0,
          val last = job.steps[job.steps.length() - 1]
          all {
            last.kind != "sleep",
            if (job.leased)
              quiet(state, outcome("rejected", 0, "", "nondeterministic"))
            else
              quiet(state, outcome("rejected", 0, "", "stale-lease"))
          }
        }
      }
  }

  action complete = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          not(job.waiting),
          val terminal = {
            value: "done",
            lease: job.lease,
            position: state.revision + 1,
          }
          commit(
            state,
            { ...emptyRecord, kind: "complete", key: key, terminal: terminal },
            [terminalIntent(key, terminal)],
            outcome("accepted", state.revision + 1, "completed", ""),
          )
        }
      }
  }

  action duplicateComplete = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.has_terminal,
          quiet(state, outcome("duplicate", job.terminal.position, "completed", ""))
        }
      }
  }

  action revoke = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          commit(
            state,
            { ...emptyRecord, kind: "revoke", key: key, lease: job.lease },
            [revokeIntent(key, job.lease)],
            outcome("accepted", state.revision + 1, "park", ""),
          )
        }
      }
  }

  // The leased invocation reaches a sleep at its next step.
  action beginTimerWait = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          not(job.waiting),
          val wait = {
            step: job.steps.length(),
            timer: true,
            fire_at: TIMER_FIRE_AT,
            lease: job.lease,
            position: state.revision + 1,
          }
          commit(
            state,
            { ...emptyRecord, kind: "wait_started", key: key, wait: wait },
            [armIntent(key, wait)],
            outcome("accepted", state.revision + 1, "park", ""),
          )
        }
      }
  }

  // The leased invocation reaches a receive at its next step. An undelivered
  // inbox message resolves it in the same commit; otherwise the job waits.
  action beginMessageWait = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          not(job.waiting),
          if (job.delivered < job.inbox.length()) {
            val resolved = {
              id: job.steps.length(),
              kind: "recv",
              name: "",
              attempt: 0,
              value: job.inbox[job.delivered].payload,
              lease: job.lease,
              position: state.revision + 1,
            }
            commit(
              state,
              { ...emptyRecord, kind: "wait_resolved", key: key, step: resolved, resolution: "queued" },
              [continueIntent(key, resolved)],
              outcome("accepted", state.revision + 1, "continue", ""),
            )
          } else {
            val wait = {
              step: job.steps.length(),
              timer: false,
              fire_at: 0,
              lease: job.lease,
              position: state.revision + 1,
            }
            commit(
              state,
              { ...emptyRecord, kind: "wait_started", key: key, wait: wait },
              [armIntent(key, wait)],
              outcome("accepted", state.revision + 1, "park", ""),
            )
          }
        }
      }
  }

  // A re-granted invocation re-attaches to the durable wait at the same step.
  action reattachWait = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.waiting,
          quiet(state, outcome("duplicate", job.wait.position, "park", ""))
        }
      }
  }

  // The current lease re-issues a wait at a step that is already committed:
  // a matching wait kind resumes with the committed value; any other step kind
  // is rejected as nondeterministic replay.
  action reattachResolvedWait = {
    nondet key = KEYS.oneOf()
    nondet timer = Set(true, false).oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.steps.length() > 0,
          val committed = job.steps[job.steps.length() - 1]
          val expected_kind = if (timer) "sleep" else "recv"
          if (committed.kind == expected_kind)
            quiet(state, outcome("duplicate", committed.position, "continue", ""))
          else
            quiet(state, outcome("rejected", 0, "", "nondeterministic"))
        }
      }
  }

  // The current lease names the held wait's step with the other wait kind:
  // the step matches a durable wait, so a differing kind is a divergent
  // replay, not an ordering violation.
  action conflictingWait = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.waiting,
          quiet(state, outcome("rejected", 0, "", "nondeterministic"))
        }
      }
  }

  action stepWhileWaiting = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.waiting,
          quiet(state, outcome("rejected", 0, "", "out-of-order"))
        }
      }
  }

  action completeWhileWaiting = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.waiting,
          quiet(state, outcome("rejected", 0, "", "out-of-order"))
        }
      }
  }

  // The host fires a due timer, whether or not the job is currently leased.
  action fireTimer = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          not(isSettled(job)),
          job.waiting,
          job.wait.timer,
          val resolved = {
            id: job.wait.step,
            kind: "sleep",
            name: "",
            attempt: 0,
            value: "null",
            lease: job.wait.lease,
            position: state.revision + 1,
          }
          commit(
            state,
            { ...emptyRecord, kind: "wait_resolved", key: key, step: resolved, resolution: "timer" },
            job.resolvedIntents(resolved),
            outcome("accepted", state.revision + 1, "continue", ""),
          )
        }
      }
  }

  // A retried timer fire for a timer wait that already resolved returns the
  // committed step receipt.
  action duplicateFire = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.steps.length() > 0,
          val committed = job.steps[job.steps.length() - 1]
          all {
            committed.kind == "sleep",
            quiet(state, outcome("duplicate", committed.position, "continue", "")),
          }
        }
      }
  }

  // A timer fires at an uncommitted step when the job holds no timer wait.
  action staleFire = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          not(job.waiting and job.wait.timer),
          if (isSettled(job))
            quiet(state, outcome("rejected", 0, "", "already-terminal"))
          else
            quiet(state, outcome("rejected", 0, "", "not-waiting"))
        }
      }
  }

  // A timer from before cancellation is fenced by the permanent receipt.
  action fireAfterCancel = {
    nondet key = KEYS.oneOf()
    all {
      state.jobs.hasJob(key),
      val job = state.jobs.findJob(key)
      all {
        job.has_fault,
        job.fault.failure == "cancelled",
        quiet(state, outcome("rejected", 0, "", "already-terminal")),
      }
    }
  }

  // A message with a fresh id (its payload is the id) is sent to `key`.
  action sendMessage = {
    nondet key = KEYS.oneOf()
    nondet id = MESSAGE_IDS.oneOf()
    val job = state.jobs.findJob(key)
    val position = state.revision + 1
    val message = { id: id, payload: id, position: position }
    all {
      not(job.inbox.hasMessage(id)),
      if (not(state.jobs.hasJob(key)))
        quiet(state, outcome("rejected", 0, "", "unknown-job"))
      else if (isSettled(job))
        quiet(state, outcome("rejected", 0, "", "already-terminal"))
      else if (job.waiting and not(job.wait.timer)) {
        val resolved = {
          id: job.wait.step,
          kind: "recv",
          name: "",
          attempt: 0,
          value: message.payload,
          lease: job.wait.lease,
          position: position,
        }
        commit(
          state,
          { ...emptyRecord, kind: "wait_resolved", key: key, step: resolved, resolution: "delivered", message: message },
          job.resolvedIntents(resolved),
          outcome("accepted", position, "admitted", ""),
        )
      } else
        commit(
          state,
          { ...emptyRecord, kind: "message_accepted", key: key, message: message },
          [],
          outcome("accepted", position, "admitted", ""),
        )
    }
  }

  // An accepted message is resent with the same payload while the job is active.
  action duplicateMessage = {
    nondet key = KEYS.oneOf()
    nondet id = MESSAGE_IDS.oneOf()
    val job = state.jobs.findJob(key)
    all {
      job.inbox.hasMessage(id),
      quiet(state, outcome("duplicate", job.inbox.findMessage(id).position, "admitted", ""))
    }
  }

  // An accepted message id is resent with a different payload.
  action conflictingMessage = {
    nondet key = KEYS.oneOf()
    nondet id = MESSAGE_IDS.oneOf()
    all {
      state.jobs.findJob(key).inbox.hasMessage(id),
      quiet(state, outcome("rejected", 0, "", "payload-conflict"))
    }
  }

  // Recovery retains receipts, waits and inboxes; only leases are volatile.
  action crash = {
    val recovered = durable.foldl(initial, (before, record) => before.applyRecord(record))
    all {
      durable' = durable,
      state' = {
        ...recovered,
        authority_epoch: state.authority_epoch + 1,
        jobs: clearLeases(recovered.jobs),
        intents: [],
        outcome: outcome("restart", 0, "", ""),
      },
    }
  }

  action snapshotRestart = all {
    durable' = durable,
    state' = {
      ...state,
      authority_epoch: state.authority_epoch + 1,
      jobs: clearLeases(state.jobs),
      intents: [],
      outcome: outcome("restart", 0, "", ""),
    },
  }

  action step = any {
    submit,
    duplicateSubmit,
    grant,
    grantDueRetry,
    commitStep,
    duplicateStep,
    staleResult,
    renamedStep,
    renamedWait,
    retry,
    retryExhausted,
    retryWhileWaiting,
    cancel,
    duplicateCancel,
    cancelSettled,
    complete,
    duplicateComplete,
    revoke,
    beginTimerWait,
    beginMessageWait,
    reattachWait,
    reattachResolvedWait,
    conflictingWait,
    stepWhileWaiting,
    completeWhileWaiting,
    fireTimer,
    duplicateFire,
    staleFire,
    fireAfterCancel,
    sendMessage,
    duplicateMessage,
    conflictingMessage,
    crash,
    snapshotRestart,
  }

  val contiguousCommittedPrefix = state.jobs.foldl(true, (valid, job) => valid and
    job.steps.indices().forall(index =>
      job.steps[index].id == index
        and Set("effect", "sleep", "recv").contains(job.steps[index].kind)
        and job.steps[index].position <= state.revision
        and (index == 0 or job.steps[index - 1].position < job.steps[index].position)
    )
  )

  val leaseSafety = state.jobs.foldl(true, (valid, job) => valid and
    job.last_generation >= 0
      and (not(job.leased) or (
        job.lease.authority_epoch == state.authority_epoch
        and job.lease.generation == job.last_generation
        and job.lease.position <= state.revision
      ))
      and (not(isSettled(job)) or not(job.leased))
  )

  // A held wait suspends exactly the job's next step, never outlives a
  // settled job, and a message wait only exists once the inbox is drained.
  val waitSafety = state.jobs.foldl(true, (valid, job) => valid and
    if (job.waiting) and {
      not(isSettled(job)),
      job.wait.step == job.steps.length(),
      job.wait.position <= state.revision,
      job.wait.lease.job_incarnation == job.incarnation,
      job.wait.timer or job.delivered == job.inbox.length(),
    } else job.wait == emptyWait
  )

  // Retries and restarts stay inside the submission budget, a parked retry is
  // never leased and has been charged to the budget, progress is only recorded
  // by a grant, and settlement clears any parked retry.
  val retrySafety = state.jobs.foldl(true, (valid, job) => valid and
    job.restarts <= MAX_RESTARTS + 1
      and job.retry_attempts <= MAX_RETRIES
      and (not(job.retrying) or (
        not(job.leased)
        and not(isSettled(job))
        and job.retry.fire_at >= 0
        and job.retry_attempts >= 1
      ))
      and (not(job.has_progress) or job.progress.steps <= job.steps.length())
      and (not(job.has_fault) or (not(job.retrying) and job.fault.position <= state.revision))
      and (job.retrying or job.retry == emptyRetryWait)
  )

  // Message ids are unique within an inbox, accepted in log order, and only
  // an accepted prefix is ever delivered.
  val inboxSafety = state.jobs.foldl(true, (valid, job) => valid and
    job.delivered >= 0
      and job.delivered <= job.inbox.length()
      and job.inbox.indices().forall(index =>
        job.inbox[index].position <= state.revision
          and (index == 0 or job.inbox[index - 1].position < job.inbox[index].position)
          and job.inbox.indices().forall(other => other == index or job.inbox[other].id != job.inbox[index].id)
      )
  )

  val rejectionHasNoIntents = state.outcome.kind != "rejected" or state.intents == []

  // Settled jobs retain only their compact permanent receipt: active step
  // history, leases, waits, retries, progress, and inboxes are dropped.
  val receiptSafety = state.jobs.foldl(true, (valid, job) => valid and
    (not(isSettled(job)) or and {
      not(job.has_terminal and job.has_fault),
      job.steps == [],
      job.inbox == [],
      job.delivered == 0,
      not(job.leased),
      job.lease == emptyLease,
      not(job.waiting),
      job.wait == emptyWait,
      not(job.retrying),
      job.retry == emptyRetryWait,
      job.retry_attempts == 0,
      job.restarts == 0,
      not(job.has_progress),
      job.progress == emptyProgress,
    })
  )

  val durabilityBoundary = state.revision == durable.length() and state.durable_len == durable.length()

  val invariant = and {
    contiguousCommittedPrefix,
    leaseSafety,
    waitSafety,
    inboxSafety,
    retrySafety,
    receiptSafety,
    rejectionHasNoIntents,
    durabilityBoundary,
  }
}

module bounded {
  import wflow_authority(KEYS = Set("a", "b"), WORKERS = Set(11, 12), MESSAGE_IDS = Set("m1", "m2")).*
}
