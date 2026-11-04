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
  // step number of the held wait, or -1 for Rust's `None`. Entering cleanup
  // is progress too: the restart budget starts over for the cleanup phase.
  type Progress = {
    steps: int,
    waiting: int,
    cleanup: bool,
  }

  // The phase a job entered when it stopped forward work. `cause` is
  // "cancelled" (an operator cancel, `reason` carries its reason), "failed"
  // (the lease holder hit a terminal error, `reason` carries it), or
  // "retries-exhausted" (the holder's transient failure found the forward
  // retry budget spent, `reason` carries that error). `steps` is the
  // committed-step count the boundary sits at; `has_lease`/`lease` is the
  // lease held when cleanup began: interrupted by a cancel (and then the only
  // one whose in-flight effect may still commit at `steps`), reporting the
  // failure, or released by the exhausted retry.
  type Cleanup = {
    cause: str,
    reason: str,
    steps: int,
    has_lease: bool,
    lease: Lease,
    position: int,
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
    has_cleanup: bool,
    cleanup: Cleanup,
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
    cleanup: Cleanup,
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
  // `cleanup` is only meaningful for "cleanup_started" records.
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
    cleanup: Cleanup,
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
    cleanup: false,
  }

  def emptyCleanup: Cleanup = {
    cause: "",
    reason: "",
    steps: 0,
    has_lease: false,
    lease: emptyLease,
    position: 0,
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
    has_cleanup: false,
    cleanup: emptyCleanup,
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
    cleanup: emptyCleanup,
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
    cleanup: emptyCleanup,
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

  def removeJob(jobs: List[Job], key: str): List[Job] = jobs.foldl([], (result, job) => if (job.key == key) result else result.append(job))

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

  def cleanupIntent(key: str, cleanup: Cleanup): Intent = {
    ...emptyIntent,
    kind: "cleanup",
    key: key,
    cleanup: cleanup,
  }

  // The job reached a final outcome, successful (`terminal`) or a classified
  // authority-side failure (`fault`).
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
    cleanup: job.has_cleanup,
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

  // Whether a step of `kind` at `step_id` under `lease` belongs to the job's
  // phase, mirroring Rust's `step_in_phase`: cleanup ("defer") steps exist
  // only once cleanup began; forward effects stop there, except the one the
  // cancelled lease was already executing, which may still commit at the
  // boundary. A terminal failure interrupts nothing, so it admits none.
  def stepInPhase(job: Job, kind: str, lease: Lease, step_id: int): bool =
    if (not(job.has_cleanup)) kind != "defer"
    else if (kind == "defer") true
    else job.cleanup.cause == "cancelled"
      and job.cleanup.has_lease
      and job.cleanup.lease == lease
      and step_id == job.cleanup.steps

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
    else if (record.kind == "cleanup_started") {
      // The pending wait, if any, is abandoned: the woken invocation never
      // resolves it and the cleanup steps take its slot. A parked retry
      // stays, but no longer gates the grant that runs the cleanup. Entering
      // cleanup is progress, so the retry budget starts over for the phase;
      // an exhausted forward retry also releases the lease it was reported
      // under, like the retry it stands in for.
      ...job,
      waiting: false,
      wait: emptyWait,
      retry_attempts: 0,
      leased: job.leased and record.cleanup.cause != "retries-exhausted",
      lease: if (record.cleanup.cause == "retries-exhausted") emptyLease else job.lease,
      has_cleanup: true,
      cleanup: record.cleanup,
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
    else if (record.kind == "expire") {
      ...applied,
      jobs: before.jobs.removeJob(record.key),
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
        // on a retry with a positive deadline is not due yet, unless it has
        // cleanup to run (`grantCleanupThroughRetry`).
        if (job.retrying) all {
          not(job.has_cleanup),
          quiet(state, outcome("rejected", 0, "", "retry-pending")),
        }
        else all {
          not(isSettled(job)),
          not(job.leased),
          grantRun(key, job, worker),
        }
      }
  }

  // Cleanup does not wait for a retry deadline: the retry budget was charged
  // when the retry was scheduled, and granting the parked job early (host
  // clock still 0) to run its cleanup is that same re-run, not a restart.
  action grantCleanupThroughRetry = {
    nondet key = KEYS.oneOf()
    nondet worker = WORKERS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.retrying,
          job.has_cleanup,
          not(isSettled(job)),
          not(job.leased),
          grantRun(key, job, worker),
        }
      }
  }

  // The host fires a due retry by re-granting with its clock at the deadline.
  // The due re-run is paid from the retry budget, so it keeps the restart
  // count instead of charging one.
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

  // Forward work ran out of its transient-error retry budget: instead of
  // parking another retry the authority moves the job into cleanup, releasing
  // the lease so the next grant runs the registered compensation on a fresh
  // budget.
  action retryExhausted = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(job.waiting),
          not(job.has_cleanup),
          job.retry_attempts >= MAX_RETRIES,
          cleanupStarted(key, job, {
            cause: "retries-exhausted",
            reason: "transient",
            steps: job.steps.length(),
            has_lease: true,
            lease: job.lease,
            position: state.revision + 1,
          }, "park"),
        }
      }
  }

  // The cleanup phase ran out of its retry budget too: the authority refuses
  // to park another retry and the lease holder must settle the job with a
  // failure, leaving the remaining compensation outstanding.
  action cleanupRetryExhausted = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(job.waiting),
          job.has_cleanup,
          job.retry_attempts >= MAX_RETRIES,
          quiet(state, outcome("rejected", 0, "", "retries-exhausted"))
        }
      }
  }

  // A transient failure reported while the job is durably suspended: the wait
  // owns the resumption, so the retry is an ordering violation.
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

  // The durable entry into cleanup shared by `cancel`, `fail`, and
  // `retryExhausted` (mirrors Rust's `cleanup_started`): the record, the intent that interrupts the
  // held lease or wakes an unleased job, and the acknowledgement at the
  // cleanup's position.
  action cleanupStarted(key: str, job: Job, cleanup: Cleanup, disposition: str): bool =
    commit(
      state,
      { ...emptyRecord, kind: "cleanup_started", key: key, incarnation: job.incarnation, cleanup: cleanup },
      [cleanupIntent(key, cleanup)],
      outcome("accepted", cleanup.position, disposition, ""),
    )

  // An operator cancels an unsettled job that is not yet cleaning up. It
  // needs no lease: a running job records the lease it interrupts, a job
  // parked on a wait abandons it, and a parked retry or unleased job is
  // woken for cleanup by the next grant.
  action cancel = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          not(isSettled(job)),
          not(job.has_cleanup),
          cleanupStarted(key, job, {
            cause: "cancelled",
            reason: "operator",
            steps: job.steps.length(),
            has_lease: job.leased,
            lease: job.lease,
            position: state.revision + 1,
          }, "admitted"),
        }
      }
  }

  // A second cancel (or a cancel of a job already cleaning up after a
  // terminal failure) acknowledges the existing cleanup without interrupting
  // it again.
  action duplicateCancel = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          not(isSettled(job)),
          job.has_cleanup,
          quiet(state, outcome("duplicate", job.cleanup.position, disposition(job), "")),
        }
      }
  }

  // A cancel of a settled job is rejected before the cleanup check, whether
  // or not it ever entered cleanup.
  action cancelSettled = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          isSettled(job),
          quiet(state, outcome("rejected", 0, "", "already-terminal")),
        }
      }
  }

  // The leased invocation hit a terminal error with compensation registered:
  // the cause is recorded before any cleanup step runs, so a crash
  // mid-cleanup resumes it instead of re-running forward work. (`fail` is a
  // Quint built-in, hence the name.)
  action failJob = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(job.has_cleanup),
          not(job.waiting),
          cleanupStarted(key, job, {
            cause: "failed",
            reason: "boom",
            steps: job.steps.length(),
            has_lease: true,
            lease: job.lease,
            position: state.revision + 1,
          }, "continue"),
        }
      }
  }

  // The lease holder reports a terminal error for a job already in cleanup
  // (cancelled under this lease, or replaying a deterministic failure): it
  // continues with the durable cause.
  action duplicateFail = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.has_cleanup,
          quiet(state, outcome("duplicate", job.cleanup.position, "continue", "")),
        }
      }
  }

  // A terminal failure reported while the job is durably suspended: the lease
  // holder cannot be parked on a wait while reporting it.
  action failWhileWaiting = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.waiting,
          quiet(state, outcome("rejected", 0, "", "out-of-order")),
        }
      }
  }

  // The step record a lease holder commits at the job's next step.
  def nextStep(job: Job, kind: str, name: str, value: str, position: int): Step = {
    id: job.steps.length(),
    kind: kind,
    name: name,
    attempt: 0,
    value: value,
    lease: job.lease,
    position: position,
  }

  // The leased invocation commits a forward effect at its next step. Forward
  // work stops once cleanup began; `effectAfterCleanup` covers that phase.
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
          not(job.has_cleanup),
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

  // The leased invocation commits a cleanup ("defer") step: the registered
  // compensation it ran once cleanup began.
  action commitDefer = {
    nondet key = KEYS.oneOf()
    nondet name = STEP_NAMES.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          job.has_cleanup,
          val committed = nextStep(job, "defer", name, "undo", state.revision + 1)
          commit(
            state,
            { ...emptyRecord, kind: "step", key: key, step: committed },
            [continueIntent(key, committed)],
            outcome("accepted", state.revision + 1, "continue", ""),
          )
        }
      }
  }

  // A cleanup step reported while the job is still doing forward work: the
  // phase check runs after the ordering and attempt checks, so a job that
  // is not waiting reaches it and is rejected on phase.
  action deferBeforeCleanup = {
    nondet key = KEYS.oneOf()
    nondet name = STEP_NAMES.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          not(job.waiting),
          not(job.has_cleanup),
          quiet(state, outcome("rejected", 0, "", "wrong-phase")),
        }
      }
  }

  // A forward effect reported once cleanup began. Exactly one may still
  // commit: the effect the cancelled lease was executing when the cancel
  // landed, at the boundary step under that same lease. Anything else (a
  // second effect, another lease, or a terminal failure's cleanup) is a
  // phase violation.
  action effectAfterCleanup = {
    nondet key = KEYS.oneOf()
    nondet name = STEP_NAMES.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          not(isSettled(job)),
          job.has_cleanup,
          if (stepInPhase(job, "effect", job.lease, job.steps.length())) {
            val committed = nextStep(job, "effect", name, "value", state.revision + 1)
            commit(
              state,
              { ...emptyRecord, kind: "step", key: key, step: committed },
              [continueIntent(key, committed)],
              outcome("accepted", state.revision + 1, "continue", ""),
            )
          } else
            quiet(state, outcome("rejected", 0, "", "wrong-phase")),
        }
      }
  }

  // The last committed step is re-issued verbatim: an effect or defer step is
  // acknowledged as a duplicate under its recorded lease; a wait-protocol
  // step can never be committed by a step command, so a current lease learns
  // its replay diverged and a stale one that its lease is gone.
  action duplicateStep = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.steps.length() > 0,
          val committed = job.steps[job.steps.length() - 1]
          if (committed.kind == "effect" or committed.kind == "defer")
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
          not(job.has_cleanup),
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
          not(job.has_cleanup),
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

  // A fresh wait (timer or receive) at the next step once cleanup began: a
  // wait is forward work, so the replayed invocation is interrupted here
  // instead of suspending again. The phase check sits before the queued
  // inbox resolution, so even an undelivered message cannot resolve it.
  action waitInCleanup = {
    nondet key = KEYS.oneOf()
    nondet timer = Set(true, false).oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.leased,
          job.has_cleanup,
          quiet(state, outcome("rejected", 0, "", "wrong-phase")),
        }
      }
  }

  // The current lease re-issues the wait it is suspended on, as a re-granted
  // invocation does after replaying up to it. The durable wait (and its
  // deadline) stands.
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
  // A settled job can still hold a stale wait (a fault keeps it and the
  // deadline wheel re-arms every recovered wait), so the fire is rejected
  // after the duplicate branch instead of resolving into a settled job.
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
  // Settled jobs take the same rejection path as `fireTimer` (the settled
  // check sits between the duplicate branch and the wait lookup).
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

  // The deadline wheel fires the timer a cancel abandoned: the wait is gone,
  // so the fire finds nothing to resolve.
  action fireAfterCancel = {
    nondet key = KEYS.oneOf()
    all {
        state.jobs.hasJob(key),
        val job = state.jobs.findJob(key)
        all {
          job.has_cleanup,
          not(isSettled(job)),
          quiet(state, outcome("rejected", 0, "", "not-waiting")),
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
      else if (job.has_cleanup)
        quiet(state, outcome("rejected", 0, "", "wrong-phase"))
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

  // A message with a fresh id sent to a job in cleanup: its wait was abandoned
  // and no new one may begin, so admitting the message would promise a
  // delivery that cannot happen. The settled check still runs first, and a
  // resent id still deduplicates (`duplicateMessage`).
  action sendInCleanup = {
    nondet key = KEYS.oneOf()
    nondet id = MESSAGE_IDS.oneOf()
    val job = state.jobs.findJob(key)
    all {
      state.jobs.hasJob(key),
      not(job.inbox.hasMessage(id)),
      not(isSettled(job)),
      job.has_cleanup,
      quiet(state, outcome("rejected", 0, "", "wrong-phase")),
    }
  }

  // An accepted message is resent with the same payload, including to a
  // job that has since completed.
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

  // A settled job receipt expires at or after its retention deadline,
  // releasing the key for a fresh incarnation on the next submit.
  action expire = {
    nondet key = KEYS.oneOf()
    all {
      state.jobs.hasJob(key),
      val job = state.jobs.findJob(key)
      all {
        isSettled(job),
        val position = state.revision + 1
        commit(
          state,
          { ...emptyRecord, kind: "expire", key: key, incarnation: job.incarnation },
          [],
          outcome("accepted", position, "completed", ""),
        )
      }
    }
  }

  // An expiry command before the retention deadline is rejected without
  // mutating the receipt.
  action expireBeforeDeadline = {
    nondet key = KEYS.oneOf()
    all {
      state.jobs.hasJob(key),
      val job = state.jobs.findJob(key)
      all {
        isSettled(job),
        quiet(state, outcome("rejected", 0, "", "retention-pending")),
      }
    }
  }

  // Recovery keeps waits and inboxes; only leases are volatile.
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
    grantCleanupThroughRetry,
    commitStep,
    commitDefer,
    deferBeforeCleanup,
    effectAfterCleanup,
    duplicateStep,
    staleResult,
    renamedStep,
    renamedWait,
    retry,
    retryExhausted,
    cleanupRetryExhausted,
    retryWhileWaiting,
    cancel,
    duplicateCancel,
    cancelSettled,
    failJob,
    duplicateFail,
    failWhileWaiting,
    complete,
    duplicateComplete,
    revoke,
    beginTimerWait,
    beginMessageWait,
    waitInCleanup,
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
    sendInCleanup,
    duplicateMessage,
    conflictingMessage,
    expire,
    expireBeforeDeadline,
    crash,
    snapshotRestart,
  }

  val contiguousCommittedPrefix = state.jobs.foldl(true, (valid, job) => valid and
    job.steps.indices().forall(index =>
      job.steps[index].id == index
        and Set("effect", "sleep", "recv", "defer").contains(job.steps[index].kind)
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
  // never leased and was charged to the budget (unless cleanup began after it
  // was parked and reset the count), progress is only recorded by a grant,
  // and a settled job carries at most one settlement (a fault clears any
  // parked retry).
  val retrySafety = state.jobs.foldl(true, (valid, job) => valid and
    job.restarts <= MAX_RESTARTS + 1
      and job.retry_attempts <= MAX_RETRIES
      and (not(job.retrying) or (
        not(job.leased)
        and not(isSettled(job))
        and job.retry.fire_at >= 0
        and (job.retry_attempts >= 1 or (job.has_cleanup and job.cleanup.position > job.retry.position))
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

  // Cleanup abandons the held wait and ends forward work: cleanup ("defer")
  // steps exist only after cleanup began, and the only forward step that may
  // follow it is the one effect the cancelled lease was already executing,
  // committed at the boundary under that lease. A cleanup the lease holder
  // began (failure or exhausted retry) records that lease.
  val cleanupSafety = state.jobs.foldl(true, (valid, job) => valid and
    if (job.has_cleanup) and {
      not(job.waiting),
      job.cleanup.position <= state.revision,
      job.cleanup.steps <= job.steps.length(),
      job.cleanup.cause == "cancelled" or job.cleanup.cause == "failed" or job.cleanup.cause == "retries-exhausted",
      job.cleanup.cause == "cancelled" or job.cleanup.has_lease,
      job.steps.indices().forall(index =>
        val committed = job.steps[index]
        if (committed.kind == "defer") committed.position > job.cleanup.position
        else committed.position < job.cleanup.position or and {
          committed.id == job.cleanup.steps,
          job.cleanup.has_lease,
          committed.lease == job.cleanup.lease,
          job.cleanup.cause == "cancelled",
        }
      ),
    } else and {
      job.steps.indices().forall(index => job.steps[index].kind != "defer"),
      job.cleanup == emptyCleanup,
    }
  )

  // Settled jobs retain only their compact receipt: active step history,
  // leases, waits, retries, progress, cleanup, and inboxes are dropped at
  // settlement.
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
      not(job.has_cleanup),
      job.cleanup == emptyCleanup,
    })
  )

  val durabilityBoundary = state.revision == durable.length() and state.durable_len == durable.length()

  val invariant = and {
    contiguousCommittedPrefix,
    leaseSafety,
    waitSafety,
    inboxSafety,
    retrySafety,
    cleanupSafety,
    receiptSafety,
    rejectionHasNoIntents,
    durabilityBoundary,
  }
}

module bounded {
  import wflow_authority(KEYS = Set("a", "b"), WORKERS = Set(11, 12), MESSAGE_IDS = Set("m1", "m2")).*
}
