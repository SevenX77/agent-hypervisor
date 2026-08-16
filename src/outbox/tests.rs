//! Contract tests for the R1 outbox transport. Assertions anchor to observable
//! boundary artifacts — durable files on disk and committed DB rows — never to the
//! module's internal state.

use super::*;
use std::fs;

fn sample_hook_record() -> OutboxRecord {
    OutboxRecord {
        event_id: new_event_id(),
        kind: OutboxKind::HookEvent,
        agent_id: "a1".to_string(),
        provider: Some("claude".to_string()),
        lifecycle_id: Some("lifecycle-a1".to_string()),
        event: Some("stop".to_string()),
        attempt_cookie: Some("job_abc:3".to_string()),
        job_id: None,
        reply_text: None,
        reason: None,
        hook_fired_at: Some(1_720_000_000),
        payload: Some(serde_json::json!({"k": "v"})),
    }
}

// ---------------------------------------------------------------------------
// R1-T1 — journal-first durable write (CP-R1.1)
// ---------------------------------------------------------------------------

#[test]
fn journal_writes_durable_json_that_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let outbox = tmp.path().join("outbox").join("a1");
    let record = sample_hook_record();

    let path = journal_record(&outbox, &record).expect("journal must succeed on a writable dir");

    // Durable file exists under the pinned `.json` name.
    assert!(path.exists(), "durable .json must exist after journal");
    assert_eq!(path.extension().and_then(|e| e.to_str()), Some("json"));

    // It round-trips back to the exact record (union wire form preserved).
    let bytes = fs::read(&path).unwrap();
    let parsed: OutboxRecord = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        parsed, record,
        "durable record must round-trip byte-for-byte"
    );
}

#[test]
fn journal_leaves_no_tmp_and_matches_scanner_glob() {
    let tmp = tempfile::tempdir().unwrap();
    let outbox = tmp.path().join("outbox").join("a1");
    let record = sample_hook_record();

    journal_record(&outbox, &record).unwrap();

    // The scanner globs `*.json`; a durable record is exactly one such file and there is
    // NO leftover `.tmp` for the scanner to trip over (F-7: writer glob == scanner glob).
    let entries: Vec<_> = fs::read_dir(&outbox)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let jsons: Vec<_> = entries.iter().filter(|n| n.ends_with(".json")).collect();
    let tmps: Vec<_> = entries.iter().filter(|n| n.ends_with(".tmp")).collect();
    assert_eq!(
        jsons.len(),
        1,
        "exactly one durable .json expected, got {entries:?}"
    );
    assert!(
        tmps.is_empty(),
        "no .tmp may survive a committed journal, got {tmps:?}"
    );
}

#[test]
fn journal_creates_missing_outbox_dir() {
    let tmp = tempfile::tempdir().unwrap();
    // Neither `outbox/` nor the per-agent subdir exists yet.
    let outbox = tmp.path().join("outbox").join("a1");
    assert!(!outbox.exists());

    journal_record(&outbox, &sample_hook_record()).expect("journal must create the outbox dir");
    assert!(outbox.is_dir());
}

#[test]
fn journal_failure_returns_error_never_silent_ok() {
    // Fault injection: the "outbox dir" path has a *file* where a parent directory is
    // required, so `create_dir_all`/write cannot succeed. The contract: journal returns
    // Err (the CLI turns this into a loud non-zero exit) — it must NEVER return Ok when
    // nothing durable landed. This is the mirror image of the exit-0-on-failed-journal bug.
    let tmp = tempfile::tempdir().unwrap();
    let blocker = tmp.path().join("blocker");
    fs::write(&blocker, b"i am a file, not a directory").unwrap();
    let outbox = blocker.join("outbox").join("a1"); // parent is a file → cannot mkdir

    let result = journal_record(&outbox, &sample_hook_record());
    assert!(
        result.is_err(),
        "a journal that cannot make its record durable MUST return Err, not silent Ok"
    );
}

#[test]
fn record_union_wire_form_round_trips_for_job_done() {
    let record = OutboxRecord {
        event_id: new_event_id(),
        kind: OutboxKind::JobDone,
        agent_id: "a2".to_string(),
        provider: Some("antigravity".to_string()),
        lifecycle_id: Some("lifecycle-a2".to_string()),
        event: None,
        attempt_cookie: Some("job_xyz:1".to_string()),
        job_id: Some("job_xyz".to_string()),
        reply_text: Some("the declared result".to_string()),
        reason: None,
        hook_fired_at: Some(1_720_000_500),
        payload: None,
    };
    let json = serde_json::to_string(&record).unwrap();
    // Absent optional fields are omitted on the wire (union schema, mostly-null elided).
    assert!(
        !json.contains("\"event\""),
        "absent field must be omitted: {json}"
    );
    assert!(
        json.contains("\"kind\":\"job_done\""),
        "kind wire string pinned: {json}"
    );
    let parsed: OutboxRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, record);
}

#[test]
fn selfcheck_id_is_recognized_as_reserved() {
    assert!(is_reserved_selfcheck_id("selfcheck:a1:boot-7"));
    assert!(!is_reserved_selfcheck_id(&new_event_id()));
}

#[test]
fn default_outbox_dir_derives_from_socket_parent() {
    // Writer (ah agent notify --socket) and scanner (ahd state_dir) must compute the same
    // path from the socket both sides already know (F-7 writer glob == scanner glob).
    let socket = std::path::Path::new("/run/ah/state/ahd.sock");
    let derived = default_agent_outbox_dir(socket, "a1").unwrap();
    assert_eq!(derived, std::path::Path::new("/run/ah/state/outbox/a1"));
    // And it matches the scanner's own per-agent path under that state dir.
    assert_eq!(
        derived,
        outbox_dir_for_agent(std::path::Path::new("/run/ah/state"), "a1")
    );
}

// ---------------------------------------------------------------------------
// JC-1 — transport dedup ledger + consume funnel (CP-R1.2)
// ---------------------------------------------------------------------------

use crate::db::Db;
use rusqlite::params;

/// A real, migrated ahd database on a temp file (WAL requires a file). The returned
/// `TempDir` must be kept alive for the DB path to stay valid.
fn test_db() -> (Db, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let db = crate::db::init(&tmp.path().join("ahd.sqlite")).unwrap();
    (db, tmp)
}

/// Insert the minimal FK chain (projects → sessions → agents) so a `hook_event` record can
/// land its `events` row.
fn insert_agent(conn: &rusqlite::Connection, agent_id: &str) {
    conn.execute(
        "INSERT OR IGNORE INTO projects(id, absolute_path) VALUES ('p1', '/tmp/p1')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO sessions(id, project_id, master_pid) VALUES ('s1', 'p1', 1)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO agents(id, session_id, provider, state) VALUES (?1, 's1', 'claude', 'BUSY')",
        params![agent_id],
    )
    .unwrap();
}

fn job_done_record(job_id: &str, agent_id: &str) -> OutboxRecord {
    OutboxRecord {
        event_id: new_event_id(),
        kind: OutboxKind::JobDone,
        agent_id: agent_id.to_string(),
        provider: Some("claude".to_string()),
        lifecycle_id: Some(format!("lifecycle-{agent_id}")),
        event: None,
        attempt_cookie: Some(format!("{job_id}:1")),
        job_id: Some(job_id.to_string()),
        reply_text: Some("declared result".to_string()),
        reason: None,
        hook_fired_at: Some(1_720_000_000),
        payload: None,
    }
}

fn count(conn: &rusqlite::Connection, sql: &str, id: &str) -> i64 {
    conn.query_row(sql, params![id], |r| r.get(0)).unwrap()
}

#[test]
fn jc1_dedup_f3_job_done_no_double_apply() {
    // RED contract: feed the SAME job_done event_id twice (at-least-once redelivery /
    // cold-scan replay). The second must dedup at the ledger — exactly one stub row and one
    // ledger row survive. This is the g1 (Track A) JC-1 acceptance: a replayed events-path
    // record dedups; here proven on the F3 stub path too.
    let (db, _tmp) = test_db();
    let mut guard = db.conn();
    let conn = &mut *guard;

    let rec = job_done_record("job1", "a1");
    assert_eq!(
        consume_record(conn, &rec).unwrap(),
        ConsumeOutcome::FirstSeen
    );
    assert_eq!(
        consume_record(conn, &rec).unwrap(),
        ConsumeOutcome::Duplicate,
        "a replayed job_done must dedup, not re-apply"
    );

    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM outbox_job_declaration_stub WHERE event_id=?1",
            &rec.event_id
        ),
        1,
        "replayed job_done must not double-apply to the F3 sink"
    );
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM outbox_consumed WHERE event_id=?1",
            &rec.event_id
        ),
        1,
    );
}

#[test]
fn jc1_dedup_f2_hook_event_no_double_apply() {
    // The F2 events-path arm of the SAME single ledger: a replayed hook_event lands exactly
    // one `events` row.
    let (db, _tmp) = test_db();
    let mut guard = db.conn();
    let conn = &mut *guard;
    insert_agent(conn, "a1");

    let rec = sample_hook_record();
    assert_eq!(
        consume_record(conn, &rec).unwrap(),
        ConsumeOutcome::FirstSeen
    );
    assert_eq!(
        consume_record(conn, &rec).unwrap(),
        ConsumeOutcome::Duplicate
    );

    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM events WHERE agent_id=?1 AND event_type='hook_event'",
            "a1"
        ),
        1,
        "replayed hook_event must not double-insert into events"
    );
}

#[test]
fn jc1_dedup_is_keyed_on_event_id_not_job() {
    // Two DIFFERENT event_ids for the SAME job both apply — the ledger keys on the outbox
    // event_id, so distinct deliveries are distinct effects (not collapsed by job_id).
    let (db, _tmp) = test_db();
    let mut guard = db.conn();
    let conn = &mut *guard;

    let a = job_done_record("job1", "a1");
    let b = job_done_record("job1", "a1"); // same job, fresh event_id
    assert_eq!(consume_record(conn, &a).unwrap(), ConsumeOutcome::FirstSeen);
    assert_eq!(consume_record(conn, &b).unwrap(), ConsumeOutcome::FirstSeen);
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM outbox_job_declaration_stub WHERE job_id=?1",
            "job1"
        ),
        2,
    );
}

// ---------------------------------------------------------------------------
// R1-T2 / R1-T3 — cold-scan replay, reap-after-commit, dead-letter quarantine
// ---------------------------------------------------------------------------

fn hook_record_with_id(event_id: &str, agent_id: &str) -> OutboxRecord {
    OutboxRecord {
        event_id: event_id.to_string(),
        kind: OutboxKind::HookEvent,
        agent_id: agent_id.to_string(),
        provider: Some("claude".to_string()),
        lifecycle_id: Some(format!("lifecycle-{agent_id}")),
        event: Some("stop".to_string()),
        attempt_cookie: None,
        job_id: None,
        reply_text: None,
        reason: None,
        hook_fired_at: None,
        payload: None,
    }
}

fn json_files(dir: &std::path::Path) -> Vec<String> {
    if !dir.exists() {
        return vec![];
    }
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().into_owned();
            name.ends_with(".json").then_some(name)
        })
        .collect();
    v.sort();
    v
}

#[test]
fn cold_scan_replays_all_records_and_reaps_after_commit() {
    // DF-A1 core: records written to the outbox (as if before a kill -9) are all replayed on
    // the next scan (no holes) and each file is reaped only after its effect committed.
    let (db, tmp) = test_db();
    let outbox = tmp.path().join("outbox").join("a1");
    {
        let mut guard = db.conn();
        insert_agent(&guard, "a1");
        drop(guard);
    }
    journal_record(&outbox, &hook_record_with_id("01", "a1")).unwrap();
    journal_record(&outbox, &hook_record_with_id("02", "a1")).unwrap();
    journal_record(&outbox, &hook_record_with_id("03", "a1")).unwrap();

    let mut guard = db.conn();
    let report = cold_scan_dir(&mut guard, &outbox).unwrap();

    assert_eq!(report.consumed, 3, "all three must replay: no holes");
    assert_eq!(report.quarantined, 0);
    assert_eq!(
        count(
            &guard,
            "SELECT COUNT(*) FROM events WHERE agent_id=?1 AND event_type='hook_event'",
            "a1"
        ),
        3
    );
    assert!(
        json_files(&outbox).is_empty(),
        "all files reaped after commit"
    );
}

#[test]
fn cold_scan_replay_of_crash_surviving_file_does_not_double_apply() {
    // Crash-between-commit-and-reap arm (DF-A1): a record is consumed (ledger committed) but
    // its file survives (ahd died before reap). The next cold-scan re-reads the file and must
    // dedup it — exactly one events row, file finally reaped.
    let (db, tmp) = test_db();
    let outbox = tmp.path().join("outbox").join("a1");
    let rec = hook_record_with_id("01", "a1");
    journal_record(&outbox, &rec).unwrap();

    let mut guard = db.conn();
    insert_agent(&guard, "a1");
    // Apply once directly (commit) but DO NOT reap the file → simulates the crash window.
    assert_eq!(
        consume_record(&mut guard, &rec).unwrap(),
        ConsumeOutcome::FirstSeen
    );
    assert_eq!(
        json_files(&outbox).len(),
        1,
        "file still present (unreaped)"
    );

    let report = cold_scan_dir(&mut guard, &outbox).unwrap();
    assert_eq!(
        report.duplicates, 1,
        "surviving file must dedup, not re-apply"
    );
    assert_eq!(
        count(
            &guard,
            "SELECT COUNT(*) FROM events WHERE agent_id=?1 AND event_type='hook_event'",
            "a1"
        ),
        1,
        "no double-apply after replay"
    );
    assert!(json_files(&outbox).is_empty(), "deduped file is reaped");
}

#[test]
fn cold_scan_quarantines_malformed_file_without_stalling_siblings() {
    // R1-deadletter: a malformed outbox file lands in outbox/dead/ with the scan continuing —
    // a good sibling in the same dir still applies.
    let (db, tmp) = test_db();
    let outbox = tmp.path().join("outbox").join("a1");
    fs::create_dir_all(&outbox).unwrap();
    fs::write(outbox.join("garbage.json"), b"{ this is not valid json").unwrap();
    journal_record(&outbox, &hook_record_with_id("01", "a1")).unwrap();

    let mut guard = db.conn();
    insert_agent(&guard, "a1");
    let report = cold_scan_dir(&mut guard, &outbox).unwrap();

    assert_eq!(report.consumed, 1, "the good sibling still applies");
    assert_eq!(report.quarantined, 1, "the malformed file is quarantined");
    let dead = outbox.join(DEAD_LETTER_DIR);
    assert!(
        dead.join("garbage.json").exists(),
        "malformed file moved to dead/"
    );
    assert!(
        json_files(&outbox).is_empty(),
        "good file reaped, garbage moved out"
    );
}

#[test]
fn cold_scan_error_books_unapplyable_record_after_n_retries() {
    // Un-applyable record: a hook_event for an agent that does not exist (FK failure). It is
    // NOT dropped and NOT hot-looped: it is retried across scans and quarantined only after
    // MAX_APPLY_ATTEMPTS. (DF-A1 error-book-isolation approximation.)
    let (db, tmp) = test_db();
    let outbox = tmp.path().join("outbox").join("ghost");
    // deliberately DO NOT insert agent "ghost" → every apply fails the FK.
    journal_record(&outbox, &hook_record_with_id("01", "ghost")).unwrap();

    let mut guard = db.conn();
    // First MAX-1 scans defer (file stays put, nothing in dead/).
    for pass in 1..MAX_APPLY_ATTEMPTS {
        let report = cold_scan_dir(&mut guard, &outbox).unwrap();
        assert_eq!(
            report.retry_deferred, 1,
            "pass {pass} should defer, not quarantine"
        );
        assert_eq!(report.quarantined, 0);
        assert_eq!(
            json_files(&outbox).len(),
            1,
            "file retained for retry on pass {pass}"
        );
    }
    // The MAX-th scan quarantines.
    let final_report = cold_scan_dir(&mut guard, &outbox).unwrap();
    assert_eq!(
        final_report.quarantined, 1,
        "quarantined at MAX_APPLY_ATTEMPTS"
    );
    assert!(json_files(&outbox).is_empty());
    assert!(
        outbox.join(DEAD_LETTER_DIR).exists(),
        "record error-booked to dead/"
    );
}

#[test]
fn cold_scan_replays_in_event_id_order() {
    // R1-T2 ordering: replay is oldest-first by event_id. events.seq_id is AUTOINCREMENT, so
    // the order events land proves replay order. Files are written out of lexical order.
    let (db, tmp) = test_db();
    let outbox = tmp.path().join("outbox").join("a1");
    {
        let g = db.conn();
        insert_agent(&g, "a1");
    }
    // journal in a scrambled order; scanner must sort by event_id.
    journal_record(&outbox, &hook_record_with_id("03", "a1")).unwrap();
    journal_record(&outbox, &hook_record_with_id("01", "a1")).unwrap();
    journal_record(&outbox, &hook_record_with_id("02", "a1")).unwrap();

    let mut guard = db.conn();
    cold_scan_dir(&mut guard, &outbox).unwrap();

    // Pull the applied event_ids in seq_id (application) order.
    let applied: Vec<String> = {
        let mut stmt = guard
            .prepare("SELECT json_extract(payload,'$.event_id') FROM events WHERE agent_id='a1' AND event_type='hook_event' ORDER BY seq_id")
            .unwrap();
        let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.map(|r| r.unwrap()).collect()
    };
    assert_eq!(
        applied,
        vec!["01", "02", "03"],
        "replay must be event_id-ordered"
    );
}

#[test]
fn cold_scan_selfcheck_is_noop_ledgered_and_order_exempt() {
    // R1-T4: a reserved selfcheck record takes a ledger row (so a crash-surviving selfcheck
    // re-scans as a no-op) but has NO effect, and its presence does not disturb the event_id
    // ordering of the real records (ordering exemption).
    let (db, tmp) = test_db();
    let outbox = tmp.path().join("outbox").join("a1");
    {
        let g = db.conn();
        insert_agent(&g, "a1");
    }
    let selfcheck = OutboxRecord {
        event_id: "selfcheck:a1:boot-1".to_string(),
        kind: OutboxKind::Selfcheck,
        agent_id: "a1".to_string(),
        provider: Some("claude".to_string()),
        lifecycle_id: Some("lifecycle-a1".to_string()),
        event: Some("selfcheck".to_string()),
        attempt_cookie: None,
        job_id: None,
        reply_text: None,
        reason: None,
        hook_fired_at: None,
        payload: None,
    };
    journal_record(&outbox, &hook_record_with_id("01", "a1")).unwrap();
    journal_record(&outbox, &selfcheck).unwrap();
    journal_record(&outbox, &hook_record_with_id("02", "a1")).unwrap();

    let mut guard = db.conn();
    let report = cold_scan_dir(&mut guard, &outbox).unwrap();
    assert_eq!(report.consumed, 3);
    // selfcheck took a ledger row but produced no events row.
    assert_eq!(
        count(
            &guard,
            "SELECT COUNT(*) FROM outbox_consumed WHERE event_id=?1",
            "selfcheck:a1:boot-1"
        ),
        1,
        "selfcheck must take a ledger row (crash-surviving re-scan is a no-op)"
    );
    assert_eq!(
        count(
            &guard,
            "SELECT COUNT(*) FROM events WHERE agent_id=?1 AND event_type='hook_event'",
            "a1"
        ),
        2,
        "selfcheck has no side effect on the events spine"
    );

    // Re-journal the surviving selfcheck file and re-scan → dedup no-op.
    journal_record(&outbox, &selfcheck).unwrap();
    let report2 = cold_scan_dir(&mut guard, &outbox).unwrap();
    assert_eq!(
        report2.duplicates, 1,
        "surviving selfcheck re-scans as a harmless no-op"
    );
}

#[test]
fn cold_scan_all_agents_walks_every_agent_dir() {
    // cold_scan_all_agents enumerates {state_dir}/outbox/* and replays each agent's outbox.
    let (db, tmp) = test_db();
    let state_dir = tmp.path();
    {
        let g = db.conn();
        insert_agent(&g, "a1");
        g.execute(
            "INSERT INTO agents(id, session_id, provider, state) VALUES ('a2','s1','claude','BUSY')",
            [],
        )
        .unwrap();
    }
    // Distinct event_ids: the JC-1 ledger is a single GLOBAL transport dedup keyed on
    // event_id, so cross-agent ids must differ (they are globally-unique UUIDv7 in production).
    journal_record(
        &outbox_dir_for_agent(state_dir, "a1"),
        &hook_record_with_id("a1-01", "a1"),
    )
    .unwrap();
    journal_record(
        &outbox_dir_for_agent(state_dir, "a2"),
        &hook_record_with_id("a2-01", "a2"),
    )
    .unwrap();

    let report = cold_scan_all_agents(&db, state_dir).unwrap();
    assert_eq!(report.consumed, 2, "both agents' outboxes replayed");
    let guard = db.conn();
    assert_eq!(
        count(
            &guard,
            "SELECT COUNT(*) FROM events WHERE agent_id=?1 AND event_type='hook_event'",
            "a1"
        ),
        1
    );
    assert_eq!(
        count(
            &guard,
            "SELECT COUNT(*) FROM events WHERE agent_id=?1 AND event_type='hook_event'",
            "a2"
        ),
        1
    );
}

#[test]
fn jc1_dedup_and_effect_are_one_transaction() {
    // Atomicity (design R1-Q2: "dedup insert and the handler's effect commit in one
    // transaction"). A hook_event for a NON-existent agent fails the FK on the effect insert;
    // the whole tx must roll back so NO ledger row is left behind — otherwise the record
    // would be silently swallowed (a hole, not a replay). Prove the ledger is clean so a
    // later cold-scan can still retry/quarantine it.
    let (db, _tmp) = test_db();
    let mut guard = db.conn();
    let conn = &mut *guard;
    // note: no insert_agent → the events FK will reject the effect.

    let rec = sample_hook_record();
    let outcome = consume_record(conn, &rec);
    assert!(outcome.is_err(), "effect FK failure must surface as Err");
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM outbox_consumed WHERE event_id=?1",
            &rec.event_id
        ),
        0,
        "a rolled-back effect must NOT leave a committed ledger row (dedup+effect atomic)"
    );
}
