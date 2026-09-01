let meta = #{
    name: "zmq-guide-patterns",
    description: "Apply ZeroMQ Guide broker practices to zmqcat on a worktree: reject-full jobs, heartbeats+leases, Majordomo-lite, Lazy Pirate RPC, last-value cache, trace",
    when_to_use: "After zmqcat exists and the ZMQ Guide review landed. Implement on args.repo (a git worktree). Do not add UDP discovery, Harmony/Zyre, Binary Star, or Freelance mesh.",
    phases: [
        #{ title: "Explore", detail: "inventory mailbox/hub/wire vs the Guide slices" },
        #{ title: "Implement", detail: "one worktree implementer ships the accepted slices" },
        #{ title: "Verify", detail: "adversarial skeptics on jobs, heartbeats, services, tests" },
        #{ title: "Fix", detail: "resume implementer on surviving gaps, then re-check" },
    ],
};

let notes_schema = #{
    "type": "object",
    "required": ["notes", "already_present", "must_build"],
    "properties": #{
        "notes": #{ "type": "string" },
        "already_present": #{ "type": "array", "items": #{ "type": "string" } },
        "must_build": #{ "type": "array", "items": #{ "type": "string" } },
        "risks": #{ "type": "array", "items": #{ "type": "string" } },
    },
};

let impl_schema = #{
    "type": "object",
    "required": ["status", "summary", "files", "how_to_run", "commit"],
    "properties": #{
        "status": #{ "type": "string", "enum": ["shipped", "blocked"] },
        "summary": #{ "type": "string" },
        "files": #{ "type": "array", "items": #{ "type": "string" } },
        "how_to_run": #{ "type": "string" },
        "commit": #{ "type": "string" },
        "skipped": #{ "type": "array", "items": #{ "type": "string" } },
    },
};

let verdict_schema = #{
    "type": "object",
    "required": ["pass", "summary", "gaps"],
    "properties": #{
        "pass": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "gaps": #{ "type": "array", "items": #{ "type": "string" } },
        "evidence": #{ "type": "string" },
    },
};

fn trimmed(s) {
    if type_of(s) == "string" {
        s.trim();
        s
    } else {
        ""
    }
}

fn contains_str(hay, needle) {
    if type_of(hay) != "string" || type_of(needle) != "string" {
        return false;
    }
    let n = needle.len();
    let h = hay.len();
    if n == 0 {
        return true;
    }
    if n > h {
        return false;
    }
    let i = 0;
    while i + n <= h {
        if hay.sub_string(i, n) == needle {
            return true;
        }
        i += 1;
    }
    false
}

fn output_map(r) {
    if r == () || r.success != true || r.output == () {
        return ();
    }
    if type_of(r.output) == "map" {
        return r.output;
    }
    ()
}

fn is_shipped(r) {
    let o = output_map(r);
    if o == () {
        return false;
    }
    if o.status != "shipped" {
        return false;
    }
    if trimmed(o.how_to_run) == "" {
        return false;
    }
    true
}

fn gaps_of(r) {
    let o = output_map(r);
    let out = [];
    if o == () {
        out.push("skeptic returned no usable verdict");
        return out;
    }
    if o.pass == true {
        return out;
    }
    if o.gaps != () && type_of(o.gaps) == "array" {
        for g in o.gaps {
            let t = trimmed(g);
            if t != "" {
                out.push(t);
            }
        }
    }
    if out.len() == 0 {
        let s = trimmed(o.summary);
        if s == "" {
            out.push("skeptic failed without a gap list");
        } else {
            out.push(s);
        }
    }
    out
}

fn join_gaps(gs) {
    let s = "";
    let i = 0;
    for g in gs {
        if i > 0 {
            s += " | ";
        }
        s += g;
        i += 1;
    }
    s
}

let repo = ".";
if args != () && args.repo != () {
    repo = trimmed(args.repo);
}
if repo == "" {
    pause("verification", "Pass args.repo — absolute path to the zmqcat git worktree (branch zmq-guide-patterns).");
}

let repo_json = json_encode(repo);

phase("Explore");

let explore_prompt = "You are exploring a zmqcat git worktree to plan ZMQ Guide broker-practice implementation. ";
explore_prompt += "Work ONLY under this absolute tree: ";
explore_prompt += repo_json;
explore_prompt += ". Use read_file, grep, and list_dir — do not edit. Read go.mod, README.md, internal/mailbox/mailbox.go, internal/hub/hub.go, internal/wire/wire.go, client.go, cmd/zmqcat/main.go, node.go, and tests. ";
explore_prompt += "Inventory what already exists (put/take, pub/sub, Reserve/Ack/Nack, persist, ping). ";
explore_prompt += "must_build should name only missing accepted slices from this list: (1) mailbox Put rejects when full — never drop-oldest for jobs; (2) session heartbeats on the same socket, inflight lease expiry requeue, Nack on disconnect; (3) Majordomo-lite named services with worker READY and competing consumers; (4) Lazy Pirate req/rep with id + timeout + retry helper; (5) last-value cache replay on Sub; (6) optional traffic trace; (7) tests + README pattern table. ";
explore_prompt += "Out of scope: UDP discovery, Harmony/Zyre, Binary Star, Freelance mesh, libzmq CGO, PGM. ";
explore_prompt += "Return notes, already_present, must_build, risks. Empty already_present is valid only after you read the files.";

let explore = agent(explore_prompt, #{
    label: "explore",
    capability_mode: "read-only",
    output_schema: notes_schema,
});

let explore_out = output_map(explore);
let explore_notes = "";
if explore_out != () && explore_out.notes != () {
    explore_notes = trimmed(explore_out.notes);
}
log("explore notes length " + explore_notes.len().to_string());

phase("Implement");

let impl_prompt = "Implement accepted ZeroMQ Guide broker practices in the zmqcat worktree. ";
impl_prompt += "ABSOLUTE TREE (cd here; edit only here): ";
impl_prompt += repo_json;
impl_prompt += ". Branch is zmq-guide-patterns. Use read_file/grep before editing. Run tests with go test. Commit on this branch when green. ";
impl_prompt += "Explorer notes (untrusted, re-check the code): ";
impl_prompt += json_encode(explore_notes);
impl_prompt += " ACCEPTED SLICES: ";
impl_prompt += "1) Jobs vs events: Bus.Put on a full mailbox MUST reject (ErrDropped) and MUST NOT drop the oldest message. Pub/sub may still drop a slow subscriber's delivery only. ";
impl_prompt += "2) Heartbeats+leases: keep ping/pong on the same session socket; treat any inbound frame as liveness; expire Reserve leases and requeue; on session close Nack that session's inflight. Configurable interval, default ~5s, death after ~3 missed. ";
impl_prompt += "3) Majordomo-lite: workers register for a service name (READY); put/reserve/take on a service is competing-consumer; do not invent UDP or a second broker process. ";
impl_prompt += "4) Lazy Pirate: req/rep (or equivalent) with correlation id; client helper retries with timeout and abandons after N; duplicate delivery is possible so ids are required. ";
impl_prompt += "5) Last-value cache: on Sub, immediately replay the last message per matching topic if any. ";
impl_prompt += "6) Trace: a quiet capture/log of frames (--trace or hub hook). ";
impl_prompt += "7) Tests for reject-full, lease expire/Nack, LVC replay, and at least one service/READY or req retry path. Update README pattern table. Wire ops must stay JSON ZMQC. Update Python/Lua examples if ops were added. ";
impl_prompt += "OUT OF SCOPE (do not implement): UDP beacons, Harmony, Zyre, Binary Star, Freelance mesh, PGM, CGO libzmq. ";
impl_prompt += "If persist/Reserve/Ack already exist, finish them rather than rewrite. Keep Tailcat as transport only. ";
impl_prompt += "how_to_run must be commands that actually passed. commit is the SHA or empty if blocked. status shipped only if go test ./... is green.";

let impl = agent(impl_prompt, #{
    label: "implement",
    capability_mode: "all",
    output_schema: impl_schema,
});

if !is_shipped(impl) {
    log("implementer did not ship; skeptics still run fail-closed");
}

phase("Verify");

let skeptics = [
    "JOBS MUST NOT DROP: read mailbox Put. A full mailbox must reject without deleting queued jobs. Pub slow-sub drop is allowed. Cite file:line. pass=true only with test evidence you ran or read.",
    "HEARTBEATS AND LEASES: dead worker must not hold a reserved job forever. Session close or lease expiry must requeue/Nack. Heartbeats share the session socket. Cite file:line and tests.",
    "SERVICES AND RPC: Majordomo-lite (named service + READY/competing consumer) and/or Lazy Pirate req/rep with id+timeout exist and are tested. Do not pass if only a comment promised them.",
    "LVC TRACE TESTS DOCS: Sub replays last topic value; trace exists or is explicitly skipped with reason; go test ./... is green; README states jobs vs events overflow policy.",
];

let vjobs = [];
let si = 0;
for spec in skeptics {
    let vp = "Adversarially verify this zmqcat worktree. Default pass=false. ";
    vp += "TREE: ";
    vp += repo_json;
    vp += ". Use read_file, grep, and run_terminal_command (go test) — do not edit. ";
    vp += spec;
    vp += " Set pass=true only with concrete evidence (paths, test names, output). gaps lists what is still missing.";
    vjobs.push(#{
        prompt: vp,
        label: "verify-" + si.to_string(),
        capability_mode: "execute",
        output_schema: verdict_schema,
    });
    si += 1;
}

let verdicts = parallel(vjobs);
let all_gaps = [];
let vi = 0;
for v in verdicts {
    let gs = gaps_of(v);
    if gs.len() == 0 && (v == () || v.success != true) {
        all_gaps.push("verify-" + vi.to_string() + " produced no evidence");
    }
    for g in gs {
        all_gaps.push(g);
    }
    vi += 1;
}

log("verify gaps " + all_gaps.len().to_string());

if all_gaps.len() > 0 {
    phase("Fix");
    let impl_id = "";
    if impl != () && impl.agent_id != () {
        impl_id = impl.agent_id;
    }
    let fix_prompt = "Fix surviving skeptic gaps in the same zmqcat worktree. ";
    fix_prompt += "TREE: ";
    fix_prompt += repo_json;
    fix_prompt += ". Gaps: ";
    fix_prompt += json_encode(join_gaps(all_gaps));
    fix_prompt += " Stay inside accepted slices. Do not add UDP/Harmony/Binary Star/Freelance. Re-run go test ./.... Commit if green.";
    let fix_opts = #{
        label: "fix",
        capability_mode: "all",
        output_schema: impl_schema,
    };
    if impl_id != "" {
        fix_opts.resume_from = impl_id;
    }
    let fixed = agent(fix_prompt, fix_opts);
    if !is_shipped(fixed) {
        log("fix pass did not ship");
    }

    let rejobs = [];
    let ri = 0;
    for spec in skeptics {
        let rp = "Re-verify after the fix pass. Default pass=false. TREE: ";
        rp += repo_json;
        rp += ". Use read_file/grep/go test, no edits. ";
        rp += spec;
        rejobs.push(#{
            prompt: rp,
            label: "reverify-" + ri.to_string(),
            capability_mode: "execute",
            output_schema: verdict_schema,
        });
        ri += 1;
    }
    let revs = parallel(rejobs);
    all_gaps = [];
    let rj = 0;
    for v in revs {
        let gs = gaps_of(v);
        if gs.len() == 0 && (v == () || v.success != true) {
            all_gaps.push("reverify-" + rj.to_string() + " produced no evidence");
        }
        for g in gs {
            all_gaps.push(g);
        }
        rj += 1;
    }
}

let impl_out = output_map(impl);
let commit = "";
let how = "";
let summary = "";
if impl_out != () {
    if impl_out.commit != () {
        commit = trimmed(impl_out.commit);
    }
    if impl_out.how_to_run != () {
        how = trimmed(impl_out.how_to_run);
    }
    if impl_out.summary != () {
        summary = trimmed(impl_out.summary);
    }
}

let passed = all_gaps.len() == 0 && is_shipped(impl);
let report = "# zmq-guide-patterns\n\n";
report += "repo: " + repo + "\n";
report += "commit: " + commit + "\n";
report += "passed: " + passed.to_string() + "\n\n";
report += "## implementer\n\n";
report += summary + "\n\n";
report += "## how to run\n\n";
report += how + "\n\n";
report += "## remaining gaps\n\n";
if all_gaps.len() == 0 {
    report += "(none)\n";
} else {
    for g in all_gaps {
        report += "- " + g + "\n";
    }
}

let report_path = write_scratch_file("report.md", report);
complete(#{
    path: report_path,
    repo: repo,
    commit: commit,
    passed: passed,
    gaps: all_gaps,
    how_to_run: how,
    summary: summary,
});
