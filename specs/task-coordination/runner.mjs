import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync, copyFileSync, readdirSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const quint = join(root, 'node_modules/.bin/quint');
const output = join(root, '.verification');
mkdirSync(output, { recursive: true });
const files = ['register.qnt', 'register_checks.qnt', 'register_validation.qnt', 'register_convergence.qnt'];
const mode = process.argv[2];

// flock owns an OS advisory lock across the whole runner, including mutants.
// --no-fork execs Node with the locked descriptor; exit/signal closes it.
// Direct quint invocations must be serialized separately with this same lock.
const portLock = join('/tmp', `townframe-quint-apalache-8822-${process.getuid()}.lock`);
if (process.argv[3] !== '--port-locked') {
  const result = spawnSync('flock', ['--nonblock', '--conflict-exit-code', '75', '--no-fork',
    portLock, process.execPath, fileURLToPath(import.meta.url), mode ?? '', '--port-locked'],
    { stdio: 'inherit' });
  if (result.error) {
    console.error(`Verification infrastructure: flock unavailable: ${result.error.message}`);
    process.exit(1);
  }
  if (result.status === 75) console.error('Verification infrastructure: Apalache port 8822 runner lock is busy; no gate was run.');
  process.exit(result.status ?? 1);
}


function run(name, args, { cwd = root, negative = false, evidence = /\[violation\]|Error \[QNT50[89]\]/, env = process.env, timeoutMs = 300_000 } = {}) {
  console.log(`\n=== ${name}: quint ${args.join(' ')} ===`);
  const result = spawnSync(quint, args, { cwd, env, encoding: 'utf8', timeout: timeoutMs, maxBuffer: 32 * 1024 * 1024 });
  const text = (result.stdout ?? '') + (result.stderr ?? '');
  writeFileSync(join(output, `${name}.log`), text);
  process.stdout.write(text);
  if (result.error) throw result.error;
  if (negative) {
    if (result.status !== 1 || !evidence.test(text)) throw new Error(`${name}: expected a property violation, got exit ${result.status}`);
    console.log(`${name}: expected violation observed (not a parser/tool failure)`);
  } else if (result.status !== 0) {
    throw new Error(`${name}: exit ${result.status}; see .verification/${name}.log`);
  }
}
const tlc = ['verify', '--backend', 'tlc', '--tlc-config', join(root, 'tlc.json')];
function verify(name, main, propertyFlag, property, file, negative = false) {
  run(name, [...tlc, '--main', main, propertyFlag, property, file], { negative });
}

try {
  if (mode === 'handoff') {
    const env = { ...process.env, JAVA_TOOL_OPTIONS: `${process.env.JAVA_TOOL_OPTIONS ?? ''} -Xss16m -javaagent:${join(output,'quint-json-limit.jar')}` };
    run('handoff-tests',['test','--backend','typescript','--main','register_handoff','--seed','1','register_handoff.qnt']);
    run('handoff-safety',[...tlc,'--main','register_handoff','--invariant','safety','register_handoff.qnt'],{env});
    run('handoff-local-transfers',[...tlc,'--main','register_handoff','--temporal','localTransfers','register_handoff.qnt'],{env});
    for (const [name,from,to] of [
      ['checkpoint-before-publication','s.batch.get(n).subseteq(s.data.get(n).get(3))','true'],
      ['old-writer-after-selection','s.selected.get(n) == 0,\n    not(s.data.get(n).get(0).contains(id))','true,\n    not(s.data.get(n).get(0).contains(id))'],
      ['reseal-ciphertext','projected(Set(id))))),\n      copied:','reg::singleton(reg::rep(id,1,id,1,0,id,99,1))))),\n      copied:']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        for (const file of ['register.qnt','register_handoff.qnt']) copyFileSync(join(root,file),join(temporary,file));
        const source = readFileSync(join(temporary,'register_handoff.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'register_handoff.qnt'),source.replace(from,to));
        run(`handoff-${name}`,[...tlc,'--main','register_handoff','--invariant','safety','register_handoff.qnt'],{cwd:temporary,env,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'continuation-repair') {
    run('continuation-repair-tests',['test','--backend','typescript','--main','continuation_gate_repair','--seed','1','continuation_gate_repair.qnt']);
    verify('continuation-repair-safety','continuation_gate_repair','--invariant','safety','continuation_gate_repair.qnt');
    for (const [name,from,to] of [
      ['early-context-sample','s.bound and s.now > s.binding','s.bound and s.contextAt > s.binding'],
      ['release-forgets-binding',"s' = { ...s, held: false }","s' = { ...s, held: false, bound: false }"],
      ['missing-restore-binding','s.bound, not(s.active)','not(s.active)'],
      ['cache-bypasses-expiry','val expired = s.bound and s.now > s.binding','val expired = not(s.cache) and s.bound and s.now > s.binding']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'continuation_gate_repair.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'continuation_gate_repair.qnt'),source.replace(from,to));
        run(`continuation-repair-${name}`,[...tlc,'--main','continuation_gate_repair','--invariant','safety','continuation_gate_repair.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'continuations') {
    run('continuation-witnesses',['test','--backend','typescript','--main','continuation_admission','--seed','1','continuation_admission.qnt']);
    verify('continuation-required-contract','continuation_admission','--invariant','preActionAuthorization','continuation_admission.qnt');
    run('continuation-current-source-counterexample',[...tlc,'--main','continuation_admission','--init','initActual','--invariant','preActionAuthorization','continuation_admission.qnt'],{negative:true});
    const temporary = mkdtempSync(join(output,'mutant-'));
    try {
      const source = readFileSync(join(root,'continuation_admission.qnt'),'utf8');
      writeFileSync(join(temporary,'continuation_admission.qnt'),source.replace('s.now <= deadline(s)','s.now <= s.taskDeadline'));
      run('continuation-ignore-inherited-deadline',[...tlc,'--main','continuation_admission','--invariant','preActionAuthorization','continuation_admission.qnt'],{cwd:temporary,negative:true});
    } finally { rmSync(temporary,{recursive:true,force:true}); }
  } else if (mode === 'descendants') {
    run('descendant-tests',['test','--backend','typescript','--main','descendant_authorization','--seed','1','descendant_authorization.qnt']);
    verify('descendant-safety','descendant_authorization','--invariant','safety','descendant_authorization.qnt');
    for (const [name,from,to] of [
      ['renew-child','deadlines: s.deadlines.set(child,s.deadlines.get(parent))','deadlines: s.deadlines.set(child,s.deadlines.get(parent) + 1)'],
      ['late-sql-success','val success = not(s.cancellation.contains(id)) and s.now <= s.deadlines.get(id)','val success = not(s.cancellation.contains(id))'],
      ['late-managed-publication','s.now <= s.deadlines.get(id), not(s.effects.contains','true, not(s.effects.contains'],
      ['late-start','not(s.running.contains(id)), not(s.cancellation.contains(id)), s.now <= s.deadlines.get(id)','not(s.running.contains(id)), not(s.cancellation.contains(id)), true']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'descendant_authorization.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'descendant_authorization.qnt'),source.replace(from,to));
        run(`descendant-mutant-${name}`,[...tlc,'--main','descendant_authorization','--invariant','safety','descendant_authorization.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'triage-delivery') {
    run('triage-activation-tests',['test','--backend','typescript','--main','triage_activation_checks','--seed','1','triage_activation.qnt']);
    run('triage-delivery-tests',['test','--backend','typescript','--main','triage_delivery','--seed','1','triage_delivery.qnt']);
    verify('triage-activation-safety','triage_activation_checks','--invariant','safety','triage_activation.qnt');
    verify('triage-delivery-safety','triage_delivery','--invariant','safety','triage_delivery.qnt');
    verify('triage-delivery-progress','triage_delivery','--temporal','progress','triage_delivery.qnt');
    for (const [name,from,to,test] of [
      ['withdraw-pending-manifest','if (enabledPending) s.actor else','if (enabledPending) false else','enabledManifestPendingRetainsActorTest'],
      ['receipt-before-capture-settlement','s.online, s.outbox, s.settled,','s.online, s.outbox,','']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        for (const f of readdirSync(root).filter(f => f.endsWith('.qnt'))) copyFileSync(join(root,f),join(temporary,f));
        const source = readFileSync(join(temporary,'triage_delivery.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'triage_delivery.qnt'),source.replace(from,to));
        run(`triage-mutant-${name}`,test
          ? ['test','--backend','typescript','--main','triage_delivery','--seed','1','--match',test,'triage_delivery.qnt']
          : [...tlc,'--main','triage_delivery','--invariant','safety','triage_delivery.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'guards') {
    for (const [main,file] of [['router_checks','router_checks.qnt'],['task_ticket_checks','task_ticket_checks.qnt']])
      run(`guards-${main}`,['test','--backend','typescript','--main',main,'--seed','1','--match','guard.*',file]);
    for (const [name,file,main,from,to,test] of [
      ['duplicate-first-wins','router_machine.qnt','router_checks','active.filter(a => a.task != entry.task).union(Set(entry))','if (active.exists(a => a.task == entry.task)) active else active.union(Set(entry))','guardOrderedDuplicateRegistrationTest'],
      ['wrong-accepted-attempt','router_machine.qnt','router_checks','a.attempt == 0 or a.attempt == e.attempt','true','guardWrongAcceptedAttemptTest'],
      ['cancelled-start','router_machine.qnt','router_checks','if (a.cancel) 7 else 6','6','guardCancelledAcceptanceTest'],
      ['failed-claim-token','router_machine.qnt','router_checks','s.persisted or s.own.generation != e.key._1 or s.own.claim != e.key._2','s.persisted','guardClaimFailureIdentityTest'],
      ['readiness-unavailable','router_machine.qnt','router_checks','Set(1,3).contains(b.reason)','Set(1,3,7).contains(b.reason)','guardReadinessReasonFenceTest'],
      ['same-writer-success-regression','task_ticket.qnt','task_ticket_checks','else if (b.sequence > a.sequence) b else a','else if (b.sequence < a.sequence) b else a','guardSameWriterSuccessSequenceTest'],
      ['invalid-declaration-acceptance','task_ticket.qnt','task_ticket_checks','else if (not(valid(b.declaration))) 4','else if (false) 4','guardInvalidBatchPrecedesMutationTest']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        for (const f of readdirSync(root).filter(f => f.endsWith('.qnt'))) copyFileSync(join(root,f),join(temporary,f));
        const source = readFileSync(join(temporary,file),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,file),source.replace(from,to));
        run(`guards-mutant-${name}`,['test','--backend','typescript','--main',main,'--seed','1','--match',test,`${main}.qnt`],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'lifecycle') {
    const file = 'lifecycle_composition.qnt';
    const main = 'lifecycle_composition';
    const env = { ...process.env, JAVA_TOOL_OPTIONS: `${process.env.JAVA_TOOL_OPTIONS ?? ''} -Xss16m -javaagent:${join(output,'quint-json-limit.jar')}` };
    run('lifecycle-typecheck',['typecheck',file]);
    run('lifecycle-tests',['test','--backend','typescript','--main',main,'--seed','1',file]);
    run('lifecycle-safety',[...tlc,'--main',main,'--step','boundedStep','--invariant','safety',file],{env});
    for (const property of ['progress','observerIndependentDelivery'])
      run(`lifecycle-${property}`,[...tlc,'--main',main,'--init','initReplay','--temporal',property,file],{env});
    run('lifecycle-unfair',[...tlc,'--main',main,'--init','initReplay','--temporal','unfairDelivery',file],{env,negative:true});
    for (const [name,from,to,test] of [
      ['invent-durable-admission-queue','queued: s.queued.intersect(s.prepared),','queued: s.queued,','crashDropsUncommittedAdmissionTest'],
      ['receipt-before-delivery','not(s.outbox.contains(id)), not(s.acks','true, not(s.acks','receiptRequiresIntegrationTest'],
      ['forget-consumed-identities','hooks: Set(), sent: Set() } }','hooks: Set(), sent: Set(), received: Set() } }','crashAfterConsumedReplyTest'],
      ['requested-not-actual-winner','requested == 2 and s.now > authorization(s.competing,id).deadline','false','sqlExpiryWinnerTest'],
      ['settle-current-capture','slots::settle(s.slot,1,(1,id))','slots::settle(s.slot,1,(if (s.advanced) 2 else 1,id))','lateSettlementExactCaptureTest']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        for (const f of readdirSync(root).filter(f => f.endsWith('.qnt'))) copyFileSync(join(root,f),join(temporary,f));
        const source = readFileSync(join(temporary,file),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,file),source.replace(from,to));
        // Safety exploration catches receipt publication before delivery; the
        // named other witnesses force crash and deadline boundary schedules.
        const args = name === 'receipt-before-delivery'
          ? [...tlc,'--main',main,'--step','boundedStep','--invariant','safety',file]
          : ['test','--backend','typescript','--main',main,'--seed','1','--match',test,file];
        run(`lifecycle-mutant-${name}`,args,{cwd:temporary,env,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'reducers') {
    for (const [main,file] of [['router_checks','router_checks.qnt'],['worker_checks','worker_checks.qnt'],['processor_slot_checks','processor_slots.qnt'],['triage_activation_checks','triage_activation.qnt']])
      run(`test-${main}`,['test','--backend','typescript','--main',main,'--seed','1',file]);
    verify('activation-safety','triage_activation_checks','--invariant','safety','triage_activation.qnt');
    verify('processor-safety','processor_slot_checks','--invariant','safety','processor_slots.qnt');
    const env = { ...process.env, JAVA_TOOL_OPTIONS: `-javaagent:${join(output,'quint-json-limit.jar')}` };
    run('worker-progress',[...tlc,'--main','worker_progress','--temporal','progress','reducer_progress.qnt'],{env});
    run('worker-unfair-progress',[...tlc,'--main','worker_progress','--temporal','unfairProgress','reducer_progress.qnt'],{env,negative:true});
    for (const [name,file,main,from,to,test] of [
      ['worker-admission','worker_machine.qnt','worker_checks','and a.startEmitted.contains(t) and a.cancel == 0 and a.finishing.size() == 0','and true','cancellationBetweenEmissionAndAdmissionTest'],
      ['processor-settle-clock','processor_slots.qnt','processor_slot_checks','settlement: t, retained: kept.union(previous)','settlement: t, evaluation: c.evaluation + 1, retained: kept.union(previous)','lateSettlementKeepsDesiredProofTest'],
      ['activation-retrospective','triage_activation.qnt','triage_activation_checks','revision > ack.boundary','revision >= ack.boundary','changedTargetSamplesBoundaryAndDisableFencesTest'],
      ['processor-forget-proof','processor_slots.qnt','processor_slot_checks','retained: kept.union(previous)','retained: Set()','lateSettlementKeepsDesiredProofTest']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        for (const f of readdirSync(root).filter(f => f.endsWith('.qnt'))) copyFileSync(join(root,f),join(temporary,f));
        const source = readFileSync(join(temporary,file),'utf8');
        if (source.split(from).length !== 2 || from === to) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,file),source.replace(from,to));
        run(name,['test','--backend','typescript','--main',main,'--seed','1','--match',test,main === 'processor_slot_checks' ? 'processor_slots.qnt' : main === 'triage_activation_checks' ? 'triage_activation.qnt' : 'worker_checks.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'admission') {
    run('waiting-cleanup-tests',['test','--backend','typescript','--main','waiting_terminal_cleanup','--seed','1','host_admission_ownership.qnt']);
    verify('admission-safety','host_admission_ownership','--invariant','safety','host_admission_ownership.qnt');
    verify('admission-progress','host_admission_ownership','--temporal','progress','host_admission_ownership.qnt');
    verify('waiting-cleanup-safety','waiting_terminal_cleanup','--invariant','safety','host_admission_ownership.qnt');
    for (const [name,from,to,main,flag,property] of [
      ['admission-caller-owns-publication','s.stage == 2,','s.stage == 2 and s.caller,','host_admission_ownership','--temporal','progress'],
      ['admission-stop-discards-queue','s.stage == 1,','s.stage == 1 and not(s.stopping),','host_admission_ownership','--temporal','progress'],
      ['admission-release-fence-at-commit','proof: true, fence: true','proof: true, fence: false','host_admission_ownership','--invariant','safety'],
      ['admission-forget-hint','stage: 3, cache: true, hint: true','stage: 3, cache: true, hint: false','host_admission_ownership','--invariant','safety'],
      ['waiting-failure-retains-row','(4,false) else','(4,owner) else','waiting_terminal_cleanup','--invariant','safety']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'host_admission_ownership.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'host_admission_ownership.qnt'),source.replace(from,to));
        run(name,[...tlc,'--main',main,flag,property,'host_admission_ownership.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
    const temporary = mkdtempSync(join(output,'mutant-'));
    try {
      const source = readFileSync(join(root,'host_admission_ownership.qnt'),'utf8');
      const from = 'if (status == 1)';
      if (source.split(from).length !== 2) throw new Error('Ambiguous Waiting status guard');
      writeFileSync(join(temporary,'host_admission_ownership.qnt'),source.replace(from,'if (true)'));
      run('waiting-stale-overwrite',['test','--backend','typescript','--main','waiting_terminal_cleanup','--seed','1','--match','staleWaitingVerdictPreservesActiveAndTerminalTest','host_admission_ownership.qnt'],{cwd:temporary,negative:true});
    } finally { rmSync(temporary,{recursive:true,force:true}); }
  } else if (mode === 'host') {
    run('completion-tests',['test','--backend','typescript','--main','completion_order','--seed','1','host_expiry_completion.qnt']);
    verify('host-expiry-safety','host_expiry','--invariant','safety','host_expiry_completion.qnt');
    verify('host-expiry-progress','host_expiry','--temporal','progress','host_expiry_completion.qnt');
    verify('completion-safety','completion_order','--invariant','safety','host_expiry_completion.qnt');
    verify('completion-delivery-gap','completion_order','--temporal','callbackDelivery','host_expiry_completion.qnt',true);
    verify('waiting-failure-owner-gap','waiting_failure_owner_gap','--invariant','safety','host_expiry_completion.qnt',true);
    verify('host-silent-hint-gap','host_silent_hint_gap','--temporal','progress','host_expiry_completion.qnt',true);
    for (const [name,from,to,main,flag,property] of [
      ['host-needs-observer','owner and cached and not(marked)','observer and owner and cached and not(marked)','host_expiry','--temporal','progress'],
      ['host-mark-before-control','owner and cancelled,','owner,','host_expiry','--invariant','safety'],
      ['completion-before-commit','action step = any { commit, returned, notify, reopen, idle }',"action step = any { commit, returned, notify, reopen, idle, all { expired' = expired, durable' = durable, owner' = owner, proof' = proof, response' = response, callback' = 1, crashed' = crashed } }",'completion_order','--invariant','safety'],
      ['completion-requested-status','callbackStatus(persisted: int): int = persisted','callbackStatus(persisted: int): int = 1','completion_order','--invariant','safety'],
      ['completion-no-write-fence','if (expired) 2 else 1','1','completion_order','--invariant','safety'],
      ['completion-discard-proof',"owner' = false, proof' = proof","owner' = false, proof' = false",'completion_order','--invariant','safety']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'host_expiry_completion.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'host_expiry_completion.qnt'),source.replace(from,to));
        run(name,[...tlc,'--main',main,flag,property,'host_expiry_completion.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'deadline') {
    run('deadline-tests',['test','--backend','typescript','--main','deadline_merge','--seed','1','deadline_merge.qnt']);
    verify('deadline-safety','deadline_merge','--invariant','safety','deadline_merge.qnt');
    for (const [name,from,to] of [
      ['deadline-early-only','val permit = authorized(now,bounded,true)','val permit = true'],
      ['deadline-visible-work',"privateCopy' = 1, visible' = visible","privateCopy' = 1, visible' = 1"],
      ['deadline-claim-seals-expiry','not(bounded) or now <= 10','claimed or not(bounded) or now <= 10']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'deadline_merge.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'deadline_merge.qnt'),source.replace(from,to));
        run(name,[...tlc,'--main','deadline_merge','--invariant','safety','deadline_merge.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'router') {
    run('router-tests',['test','--backend','typescript','--main','router_checks','--seed','1','router_checks.qnt']);
    // The pinned translator fails with an empty error at the default JVM stack.
    // A larger stack compiles the unchanged full graph; heap/parser/property
    // settings are unchanged. Keep the default-stack failure as diagnostic evidence.
    const env = { ...process.env, JAVA_TOOL_OPTIONS: `${process.env.JAVA_TOOL_OPTIONS ?? ''} -Xss16m -javaagent:${join(output,'quint-json-limit.jar')}` };
    run('router-full-safety',[...tlc,'--main','router_checks','--invariant','safety','router_checks.qnt'],{env,timeoutMs:1_800_000});
  } else if (mode === 'successor') {
    run('successor-tests',['test','--backend','typescript','--main','successor_authorization','--seed','1','successor_authorization.qnt']);
    verify('successor-safety','successor_authorization','--invariant','safety','successor_authorization.qnt');
    const identity = '(slot,capture,a.nonce,a.origin,a.deadline)';
    for (const [name,from,to,test] of [
      ['successor-replay-boundary','now <= a.deadline','now < a.deadline','lastAuthorizedSecondReplayTest'],
      ['successor-lifetime-replay','now <= a.deadline and a.deadline - a.origin == lifetime','now <= a.deadline','changedLifetimeRenewsBeforeExpiryTest'],
      ['successor-slot',identity,'(1,capture,a.nonce,a.origin,a.deadline)','slotBindingTest'],
      ['successor-capture',identity,'(slot,1,a.nonce,a.origin,a.deadline)','captureBindingTest'],
      ['successor-nonce',identity,'(slot,capture,1,a.origin,a.deadline)','nonceBindingTest'],
      ['successor-origin',identity,'(slot,capture,a.nonce,10,a.deadline)','originBindingTest'],
      ['successor-deadline',identity,'(slot,capture,a.nonce,a.origin,20)','deadlineBindingTest'],
      ['successor-authorization-id','a.task == declarationTask','true','authorizationTaskBindingTest'],
      ['successor-declaration-id','identity(slot,capture,a) == declarationTask','true','declarationTaskBindingTest'],
      ['successor-declaration-deadline','a.deadline == declarationDeadline','true','declarationDeadlineBindingTest'],
      ['successor-configured-lifetime','and a.deadline - a.origin == lifetime\n  pure def reuseAt','and true\n  pure def reuseAt','historicalLifetimeBindingTest'],
      ['successor-unverified-bytes','trusted and identity','true and identity','unverifiedBytesAreNotAuthorizationTest']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'successor_authorization.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'successor_authorization.qnt'),source.replace(from,to));
        run(name,['test','--backend','typescript','--main','successor_authorization','--seed','1','--match',test,'successor_authorization.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'ack') {
    run('ack-tests',['test','--backend','typescript','--main','domain_ack_ledger','--seed','1','domain_ack_ledger.qnt']);
    verify('ack-safety','domain_ack_ledger','--invariant','safety','domain_ack_ledger.qnt');
    verify('ack-progress','domain_ack_ledger','--temporal','progress','domain_ack_ledger.qnt');
    for (const [name,from,to,test] of [
      ['ack-first-success','acks.union(Set(requested))','if (acks.size() == 0) Set(requested) else acks','arrivalOrderAndReplayTest'],
      ['ack-canonical-only','acks.union(Set(requested))','Set(acks.fold(requested,(a,x) => if (x > a) x else a))','arrivalOrderAndReplayTest'],
      ['ack-digest-overwrite','owner == 0 or owner == digest','true','ownerDigestAndFullSummaryIdentityTest']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        const source = readFileSync(join(root,'domain_ack_ledger.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'domain_ack_ledger.qnt'),source.replace(from,to));
        run(name,['test','--backend','typescript','--main','domain_ack_ledger','--seed','1','--match',test,'domain_ack_ledger.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'ticket') {
    for (const file of ['task_ticket.qnt','task_ticket_checks.qnt','task_ticket_algebra.qnt']) {
      run(`parse-${file}`,['parse',file]);
      run(`typecheck-${file}`,['typecheck',file]);
    }
    run('ticket-tests',['test','--backend','typescript','--main','task_ticket_checks','--seed','1','task_ticket_checks.qnt']);
    verify('ticket-safety','task_ticket_checks','--invariant','safety','task_ticket_checks.qnt');
    verify('ticket-algebra','task_ticket_algebra','--invariant','algebra','task_ticket_algebra.qnt');
    for (const [name,from,to,test] of [
      ['ticket-success-priority','else if (a.fact.success and not(b.fact.success)) a','else if (a.fact.success and not(b.fact.success)) b','successSurvivesNewerCancelTest'],
      ['ticket-atomic-conflict','})) 6 else 0','})) 0 else 0','equivocationRejectsWholeBatchTest'],
      ['ticket-zero-absence','if (b.sequence == -1) a','if (b.sequence <= 0) a','zeroSequenceTerminalIsPresentTest'],
      ['ticket-evidence-loss','a.publishers.union(b.publishers)','a.publishers','samePublisherDistinctContentRetainedTest']
    ]) {
      const temporary = mkdtempSync(join(output,'mutant-'));
      try {
        for (const file of ['task_ticket.qnt','task_ticket_checks.qnt']) copyFileSync(join(root,file),join(temporary,file));
        const source = readFileSync(join(temporary,'task_ticket.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'task_ticket.qnt'),source.replace(from,to));
        run(name,['test','--backend','typescript','--main','task_ticket_checks','--seed','1','--match',test,'task_ticket_checks.qnt'],{cwd:temporary,negative:true});
      } finally { rmSync(temporary,{recursive:true,force:true}); }
    }
  } else if (mode === 'check') {
    for (const file of files) {
      run(`parse-${file}`, ['parse', file]);
      run(`typecheck-${file}`, ['typecheck', file]);
    }
    for (const [main, file] of [['register_checks','register_checks.qnt'], ['register_zero_checks','register_checks.qnt'], ['register_validation','register_validation.qnt'], ['register_convergence','register_convergence.qnt']]) {
      run(`test-${main}`, ['test','--backend','typescript','--main',main,'--seed','1',file]);
    }
  } else if (mode === 'simulate') {
    run('simulate-register', ['run','--backend','typescript','--main','register_checks','--invariant','safety','--max-steps','30','--max-samples','100','--seed','1','register_checks.qnt']);
  } else if (mode === 'verify') {
    // Keep full properties enabled: discovery of a source bug must make this
    // command fail, not silently shrink the semantic domain or weaken safety.
    verify('safety-full','register_checks','--invariant','safety','register_checks.qnt');
    verify('zero-safety','register_zero_checks','--invariant','safety','register_checks.qnt');
    verify('zero-algebra','register_zero_algebra','--invariant','algebra','register_checks.qnt');
    verify('zero-convergence','register_zero_convergence','--temporal','convergence','register_convergence.qnt');
    verify('algebra-full','register_algebra','--invariant','algebra','register_checks.qnt');
    verify('validation','register_validation','--invariant','boundaryClasses','register_validation.qnt');
    verify('convergence','register_convergence','--temporal','convergence','register_convergence.qnt');
    verify('signature-convergence','register_signature_convergence','--temporal','convergence','register_convergence.qnt');
  } else if (mode === 'facts') {
    verify('behavioral-safety','register_checks','--invariant','behavioralSafety','register_checks.qnt');
    verify('validation','register_validation','--invariant','boundaryClasses','register_validation.qnt');
    verify('convergence','register_convergence','--temporal','convergence','register_convergence.qnt');
  } else if (mode === 'algebra') {
    verify('algebra-full','register_algebra','--invariant','algebra','register_checks.qnt');
  } else if (mode === 'signature') {
    verify('signature-convergence','register_signature_convergence','--temporal','convergence','register_convergence.qnt');
  } else if (mode === 'negative') {
    verify('unfair-convergence','register_convergence','--temporal','unfairConvergence','register_convergence.qnt',true);
    const mutations = [
      ['zero-sentinel-confusion','else if (a.sequence == -1) emptyLane','else if (a.sequence <= 0) emptyLane','zeroInsertionReplayConflictRecoveryTest'],
      ['stale-replay','if (b.sequence < a.sequence) a','if (b.sequence < a.sequence) b','poisonTransferAndRecoveryTest'],
      ['drop-hidden-replay-fence',
        'pure def join(a: Register, b: Register): Register = writers.mapBy(w => joinLane(a.get(w), b.get(w)))',
        `pure def join(a: Register, b: Register): Register = {
    val merged = writers.mapBy(w => joinLane(a.get(w), b.get(w)))
    writers.mapBy(w => if (merged.get(w).representations.size() == 1 and not(heads(merged).exists(h => h.writer == w))) emptyLane else merged.get(w))
  }`,
        'hiddenReplayFenceTest'],
      ['poison-witnesses','val least = ids.fold(999,(a,s) => if (s < a) s else a)','val least = ids.fold(-1,(a,s) => if (s > a) s else a)','poisonReplacementTest'],
      ['partial-invalid-batch','b.schema and b.binding and b.uniqueWriters and b.valid','b.schema and b.binding and b.uniqueWriters','rejectedMultiLaneBatchTest'],
      ['foreign-causality','d._1 == 1 and d._2 == b.writer','d._2 == b.writer','foreignRecordDoesNotDominateTest'],
      ['wrapper-arrival-rank','a.binding < b.binding','a.binding > b.binding','canonicalOrderingAndOutcomeTest'],
      ['tampered-original','and r.header.semantic == originalSemantic','and true','localProducerAndReaderBoundaryTest'],
      ['pre-fix-signature-order',
        `if (a.header.signature != b.header.signature) a.header.signature < b.header.signature
    else if (a.binding != b.binding) a.binding < b.binding
    else if (a.publisher != b.publisher) a.publisher < b.publisher
    else if (a.ciphertext != b.ciphertext) a.ciphertext < b.ciphertext`,
        `if (a.binding != b.binding) a.binding < b.binding
    else if (a.publisher != b.publisher) a.publisher < b.publisher
    else if (a.ciphertext != b.ciphertext) a.ciphertext < b.ciphertext
    else if (a.header.signature != b.header.signature) a.header.signature < b.header.signature`,
        'signatureVariantFutureConflictTest'],
    ];
    for (const [name, from, to, test] of mutations) {
      const temporary = mkdtempSync(join(output, 'mutant-'));
      try {
        for (const file of files) copyFileSync(join(root,file),join(temporary,file));
        const source = readFileSync(join(temporary,'register.qnt'),'utf8');
        if (source.split(from).length !== 2) throw new Error(`Ambiguous mutation site: ${name}`);
        writeFileSync(join(temporary,'register.qnt'),source.replace(from,to));
        run(`mutant-${name}`,['test','--backend','typescript','--main',name === 'zero-sentinel-confusion' ? 'register_zero_checks' : 'register_checks','--seed','1','--match',test,'register_checks.qnt'],{ cwd: temporary, negative: true });
        if (name === 'pre-fix-signature-order') {
          run('mutant-pre-fix-fair-convergence',[...tlc,'--main','register_signature_convergence','--temporal','convergence','register_convergence.qnt'],{ cwd: temporary, negative: true });
        }
      } finally {
        rmSync(temporary,{ recursive: true, force: true });
      }
    }
  } else {
    throw new Error('Usage: node runner.mjs handoff|continuation-repair|continuations|descendants|triage-delivery|guards|lifecycle|reducers|admission|host|deadline|router|successor|ack|ticket|check|simulate|verify|facts|algebra|signature|negative');
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
