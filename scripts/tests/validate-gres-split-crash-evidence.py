#!/usr/bin/env python3
"""Independently recompute bounded Gres Split crash evidence invariants."""
from __future__ import annotations

import argparse, copy, hashlib, json, struct, tempfile
from pathlib import Path
from typing import Any, Callable

EXPECTED = {
    "source_restore": ["initiated_before_running_cas", "checkpoint_receipt_before_journal_cas", "checkpointed_after_journal_cas", "pause_receipt_before_journal_cas", "paused_before_stage", "stage_receipt_before_journal_cas", "staged_after_journal_cas", "marker_claim_receipt_before_journal_cas", "restored_after_journal_cas", "prologue_receipt_before_journal_cas", "activated_after_journal_cas"],
    "publication": ["tenant_cas_before_journal_cas", "layout_published_after_journal_cas"],
    "retirement_resume": ["retiring_before_delete", "delete_success_before_sidecar_cas", "parked_after_sidecar_cas", "retire_receipt_before_journal_cas", "resuming_after_journal_cas", "completed_after_journal_cas"],
}
# Engine pause bounds plus the workload's client-side ambiguity-resolution
# allowance (3s healthy-empty-read streak + polling and psql round trips).
BOUNDS = {"source_restore": 29_000, "publication": 19_000, "retirement_resume": 19_000}
INTS = {"schema_version", "acknowledged_rows", "recovered_acknowledgements", "max_ack_gap_ms", "max_ack_gap_bound_ms", "operation_elapsed_ms", "operation_bound_ms", "marker_count", "left_marker_count", "right_marker_count", "delete_count", "old_pid", "new_pid", "kill_ms", "restart_ms", "publication_ms", "left_wal_generation", "right_wal_generation", "old_source_pid", "new_source_pid", "old_source_process_group", "new_source_process_group", "workload_process_group", "operation_revision", "operation_attempts", "tenant_record_version", "source_record_version", "retirement_source_generation"}
STRINGS = {"evidence_id", "family", "case", "tenant_id", "operation_id", "sentinel_topic", "coordinator_endpoint", "left_endpoint", "right_endpoint", "marker_response_digest", "completed_phase", "operation_marker_digest", "retirement_marker_digest"}
BOOLS = {"post_publication_r2_ack", "post_publication_r3_ack", "predecessor_topic_absent", "sentinel_topic_present", "workload_process_reaped", "unrelated_delete_attempted", "new_source_pid_alive_at_verification", "old_source_pid_alive", "new_source_pid_alive", "old_source_process_group_alive", "new_source_process_group_alive", "workload_process_group_alive"}
LISTS = {"topology_topics", "payload_events", "reopened_oracle_rows", "direct_physical_rows", "sql_union_rows", "source_markers", "left_markers", "right_markers", "authenticated_receipts", "journal_receipt_expectations", "delete_attempts", "terminal_layout", "retirement_successor_generations"}
OBJECTS = {"pre_kill_predicate", "terminal_operation_evidence"}
KEYS = INTS | STRINGS | BOOLS | LISTS | OBJECTS
RECEIPTS = {"checkpoint", "pause", "stage", "markers", "prologue", "retire"}
RECEIPT_ORDER = ["checkpoint", "pause", "stage", "markers", "prologue", "retire"]
REQUEST_KIND = {"checkpoint":"force_checkpoint", "pause":"pause_at_covered_offset", "stage":"stage_filtered_restore", "markers":"inherit_markers", "prologue":"successor_fence_prologue", "retire":"retire_predecessor"}
RESPONSE_KINDS = {"checkpoint":{"checkpoint"}, "pause":{"paused"}, "stage":{"staged"}, "markers":{"markers"}, "prologue":{"applied","already_applied"}, "retire":{"applied","already_applied"}}

class ValidationError(ValueError): pass
def fail(message: str) -> None: raise ValidationError(message)
def compact(value: Any) -> bytes: return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
def digest(value: bytes) -> str: return hashlib.sha256(value).hexdigest()

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
HASH_EVIDENCE_KEYS = {"algorithm", "boundary", "corpus", "snapshots", "sql_rows", "transactions", "folds"}

def hash_bucket(raw: bytes, count: int) -> int:
    value = FNV_OFFSET
    for byte in raw:
        value = ((value ^ byte) * FNV_PRIME) & ((1 << 64) - 1)
    return value & (count - 1)

def decode_hash_row(raw_key: str, raw_value: str, label: str) -> dict[str, Any]:
    try: key=bytes.fromhex(raw_key); value=bytes.fromhex(raw_value)
    except ValueError: fail(f"{label}: raw hex is malformed")
    if len(key)!=28: fail(f"{label}: hash primary version key must be 28 bytes")
    table,index,bucket=struct.unpack(">III",key[:12]); rowid,version=struct.unpack(">QQ",key[12:])
    if table not in {50,51} or index!=1 or bucket>=16: fail(f"{label}: raw hash key class is invalid")
    if len(value)<24 or value[0]!=2 or value[1] not in {2,3}: fail(f"{label}: raw timestamp value header is invalid")
    start_ts,commit_ts=struct.unpack(">QQ",value[2:18])
    if value[1]==2 and commit_ts<=start_ts: fail(f"{label}: committed timestamp is invalid")
    if value[1]==3 and commit_ts!=0: fail(f"{label}: aborted timestamp is invalid")
    row=value[18:]
    if len(row)<6 or row[0]!=1 or row[1]!=2: fail(f"{label}: raw row lacks logical int4")
    logical_id=struct.unpack(">i",row[2:6])[0]; cur=6; seq=None; checksum=None
    if cur<len(row):
        if row[cur]!=2 or cur+5>len(row): fail(f"{label}: raw row has malformed seq")
        seq=struct.unpack(">i",row[cur+1:cur+5])[0]; cur+=5
    if cur<len(row):
        if row[cur]!=4 or cur+5>len(row): fail(f"{label}: raw row has malformed checksum")
        length=struct.unpack(">I",row[cur+1:cur+5])[0]; cur+=5
        if cur+length!=len(row): fail(f"{label}: raw row checksum length differs")
        try: checksum=row[cur:].decode()
        except UnicodeDecodeError: fail(f"{label}: raw row checksum is not UTF-8")
    if logical_id<0 or hash_bucket(struct.pack(">i",logical_id),16)!=bucket: fail(f"{label}: logical hash bucket mismatch")
    return {"table_id":table,"logical_id":logical_id,"rowid":rowid,"bucket":bucket,"version":version,"start_ts":start_ts,"commit_ts":commit_ts,"state":"committed" if value[1]==2 else "aborted","key_class":"hash_primary_version","seq":seq,"checksum":checksum}

def decode_txd2(raw_key: str, raw_value: str, label: str) -> dict[str, Any]:
    try: key=bytes.fromhex(raw_key); value=bytes.fromhex(raw_value)
    except ValueError: fail(f"{label}: transaction raw hex is malformed")
    prefix=b"\0\0\0\0meta/ts_txn/"
    if not key.startswith(prefix) or len(key)!=len(prefix)+8 or not value.startswith(b"TXD2"): fail(f"{label}: not a TXD2 record")
    start_ts=struct.unpack(">Q",key[-8:])[0]; cur=4
    def take(fmt: str) -> tuple[int,...]:
        nonlocal cur
        size=struct.calcsize(fmt)
        if cur+size>len(value): fail(f"{label}: short TXD2 record")
        result=struct.unpack(fmt,value[cur:cur+size]); cur+=size; return result
    global_xid,generation=take(">QQ")
    participants=[take(">I")[0] for _ in range(take(">I")[0])]
    prepared=[take(">I")[0] for _ in range(take(">I")[0])]
    operations=[]
    for _ in range(take(">I")[0]):
        range_id,table_id=take(">II"); tag=take(">B")[0]; bucket=take(">I")[0]; rowid=take(">Q")[0]; delete=take(">B")[0]
        if tag!=1 or bucket>=16 or delete not in {0,1}: fail(f"{label}: malformed hash TXD2 operation")
        operations.append((range_id,table_id,bucket,rowid,bool(delete)))
    remaining=value[cur:]
    if remaining==b"\0": decision="pending"
    elif remaining==b"\1": decision="aborted"
    elif len(remaining)==9 and remaining[0]==2 and struct.unpack(">Q",remaining[1:])[0]>start_ts: decision="committed"
    else: fail(f"{label}: malformed TXD2 decision")
    return {"start_ts":start_ts,"global_xid":global_xid,"generation":generation,"participants":participants,"prepared":prepared,"operations":operations,"decision":decision}

def load(path: Path) -> dict[str, Any]:
    try: value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error: fail(f"{path}: unreadable JSON: {error}")
    if not isinstance(value, dict): fail(f"{path}: root must be object")
    return value

def exact_object(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys: fail(f"{label}: object keys differ")
    return value

def rows(value: Any, located: bool, label: str) -> list[tuple[Any, ...]]:
    expected = {"table_id", "rowid", "seq", "checksum"} | ({"range_id"} if located else set())
    if not isinstance(value, list): fail(f"{label}: rows must be list")
    result = []
    for item in value:
        item = exact_object(item, expected, label)
        integer_keys = expected - {"checksum"}
        if any(type(item[key]) is not int or item[key] < 0 for key in integer_keys) or not isinstance(item["checksum"], str): fail(f"{label}: malformed row")
        result.append(tuple(item[key] for key in (["range_id"] if located else []) + ["table_id", "rowid", "seq", "checksum"]))
    if result != sorted(result) or len(result) != len(set(result)): fail(f"{label}: rows must be sorted and unique")
    return result

def marker_rows(value: Any, label: str) -> list[tuple[int, int, int | None, int]]:
    if not isinstance(value, list): fail(f"{label}: markers must be list")
    result = []
    for item in value:
        if not isinstance(item, dict) or set(item) not in ({"transaction_id", "table_id", "rowid"}, {"transaction_id", "table_id", "bucket", "rowid"}): fail(f"{label}: unexpected marker fields")
        if any(type(item[key]) is not int or item[key] < 0 for key in item): fail(f"{label}: malformed marker")
        result.append((item["transaction_id"], item["table_id"], item.get("bucket"), item["rowid"]))
    if result != sorted(result) or len(result) != len(set(result)): fail(f"{label}: markers must be sorted and unique")
    return result

def marker_digest(markers: list[tuple[int, ...]]) -> str:
    hasher = hashlib.sha256()
    for marker in markers:
        if len(marker) == 3:
            transaction_id, table_id, rowid = marker; bucket = None
        else:
            transaction_id, table_id, bucket, rowid = marker
        hasher.update(struct.pack(">Q", transaction_id)); hasher.update(struct.pack(">Q", table_id))
        if bucket is not None: hasher.update(b"\1"); hasher.update(struct.pack(">I", bucket))
        hasher.update(struct.pack(">Q", rowid))
    return hasher.hexdigest()

def expected_predicate(case: str) -> dict[str, Any]:
    source = {
        "initiated_before_running_cas": ("initiated","none",0,False),
        "checkpoint_receipt_before_journal_cas": ("running","checkpoint",0,False),
        "checkpointed_after_journal_cas": ("checkpointed","checkpoint",1,False),
        "pause_receipt_before_journal_cas": ("checkpointed","pause",1,False),
        "paused_before_stage": ("paused","pause",3,False),
        "stage_receipt_before_journal_cas": ("paused","stage",3,False),
        "staged_after_journal_cas": ("paused","stage",7,False),
        "marker_claim_receipt_before_journal_cas": ("paused","marker",7,False),
        "restored_after_journal_cas": ("restored","marker",15,False),
        "prologue_receipt_before_journal_cas": ("restored","prologue",15,False),
        "activated_after_journal_cas": ("activated","prologue",15,True),
    }
    if case in source:
        phase,receipt,evidence,serving=source[case]
        return {"phase":phase,"receipt":receipt,"evidence":evidence,"layout":"current","sidecar":"none","predecessor_topic_present":True,"delete_count":0,"successors_serving":serving}
    target = {
        "tenant_cas_before_journal_cas": ("activated","prologue","parking",True,0),
        "layout_published_after_journal_cas": ("layout_published","prologue","parking",True,0),
        "retiring_before_delete": ("retiring","prologue","parking",True,0),
        "delete_success_before_sidecar_cas": ("retiring","prologue","parking",False,1),
        "parked_after_sidecar_cas": ("retiring","prologue","parked",False,1),
        "retire_receipt_before_journal_cas": ("retiring","retire","parked",False,1),
        "resuming_after_journal_cas": ("resuming","retire","parked",False,1),
        "completed_after_journal_cas": ("completed","retire","parked",False,1),
    }
    phase,receipt,sidecar,topic,deletes=target[case]
    return {"phase":phase,"receipt":receipt,"evidence":15,"layout":"target","sidecar":sidecar,"predecessor_topic_present":topic,"delete_count":deletes,"successors_serving":True}

def hash_owner(table_id: int, logical_id: int) -> int | None:
    if table_id == 51:
        return 3
    if table_id != 50:
        return None
    bucket = hash_bucket(struct.pack(">i", logical_id), 16)
    return 0 if bucket < 4 else 2 if bucket < 8 else 3

def validate_record_v2(r: dict[str, Any], family: str, case: str, source: str, hash_mode: bool = False) -> None:
    if set(r) != KEYS: fail(f"{source}: schema keys differ: missing={sorted(KEYS-set(r))} extra={sorted(set(r)-KEYS)}")
    for key in INTS:
        if type(r[key]) is not int: fail(f"{source}: {key} must be integer")
    for key in STRINGS:
        if not isinstance(r[key], str) or not r[key]: fail(f"{source}: {key} must be nonempty string")
    for key in BOOLS:
        if type(r[key]) is not bool: fail(f"{source}: {key} must be boolean")
    for key in LISTS:
        if not isinstance(r[key], list): fail(f"{source}: {key} must be list")
    for key in OBJECTS:
        if not isinstance(r[key], dict): fail(f"{source}: {key} must be object")
    if r["schema_version"] != 2 or r["family"] != family or r["case"] != case or case not in EXPECTED[family]: fail(f"{source}: schema/family/case mismatch")
    expected_evidence_id = digest(f"{family}\0{case}\0{r['tenant_id']}\0{r['operation_id']}".encode())
    if r["evidence_id"] != expected_evidence_id: fail(f"{source}: evidence_id mismatch")

    events=[]; attempts={}; acknowledgements={}; recovered=0; ack_timestamps=[]
    for event in r["payload_events"]:
        event=exact_object(event,{"kind","provenance","table_id","rowid","seq","checksum","timestamp_ms"},"payload event")
        if event["kind"] not in {"attempt","retry","ack","recovered_ack"} or event["provenance"] not in {"seed","workload"} or type(event["table_id"]) is not int or type(event["seq"]) is not int or type(event["timestamp_ms"]) is not int or not isinstance(event["checksum"],str) or not event["checksum"]: fail(f"{source}: malformed payload event")
        events.append(event); key=(event["table_id"],event["seq"])
        if event["kind"]=="attempt":
            if event["rowid"] is not None or key in attempts: fail(f"{source}: duplicate/malformed payload attempt")
            attempts[key]=event
        elif event["kind"]=="retry":
            if event["rowid"] is not None or key not in attempts or attempts[key]["checksum"]!=event["checksum"]: fail(f"{source}: orphan/physical payload retry")
        else:
            if type(event["rowid"]) is not int or key not in attempts or attempts[key]["checksum"]!=event["checksum"] or key in acknowledgements: fail(f"{source}: orphan/duplicate payload acknowledgement")
            acknowledgements[key]=event; ack_timestamps.append(event["timestamp_ms"]); recovered += event["kind"]=="recovered_ack"
    if [event["timestamp_ms"] for event in events] != sorted(event["timestamp_ms"] for event in events): fail(f"{source}: payload event time regressed")
    computed_gap=max((right-left for left,right in zip(ack_timestamps,ack_timestamps[1:])),default=0)
    ack_rows=sorted((event["table_id"],event["rowid"],event["seq"],event["checksum"]) for event in acknowledgements.values())
    if len(ack_rows)!=r["acknowledged_rows"] or recovered!=r["recovered_acknowledgements"] or computed_gap!=r["max_ack_gap_ms"]: fail(f"{source}: payload-derived summary mismatch")
    if not any(event["timestamp_ms"]<r["kill_ms"] for event in acknowledgements.values()) or not any(event["timestamp_ms"]>r["restart_ms"] for event in acknowledgements.values()): fail(f"{source}: pre-kill/post-restart ACK proof missing")
    computed_r2=any(event["provenance"]=="workload" and event["timestamp_ms"]>r["publication_ms"] and event["table_id"]==50 and ((hash_mode and hash_owner(50,event["rowid"])==2) or (not hash_mode and event["rowid"]>=16)) for event in acknowledgements.values())
    computed_r3=any(event["provenance"]=="workload" and event["timestamp_ms"]>r["publication_ms"] and event["table_id"]==51 and event["rowid"]>=16 for event in acknowledgements.values())
    if computed_r2!=r["post_publication_r2_ack"] or computed_r3!=r["post_publication_r3_ack"] or not computed_r2 or not computed_r3: fail(f"{source}: post-publication stream proof mismatch")

    oracle = rows(r["reopened_oracle_rows"], False, "oracle")
    if oracle != ack_rows: fail(f"{source}: reopened oracle differs from payload ledger")
    expected_scans=[(0,50),(0,51),(2,50),(2,51),(3,50),(3,51)]; direct=[]
    if len(r["direct_physical_rows"])!=6: fail(f"{source}: six full direct scans required")
    for scan,expected_scan in zip(r["direct_physical_rows"],expected_scans):
        scan=exact_object(scan,{"range_id","table_id","rows"},"direct scan")
        if (scan["range_id"],scan["table_id"])!=expected_scan: fail(f"{source}: direct scan coverage/order mismatch")
        scan_rows=rows(scan["rows"],False,"direct scan rows")
        if any(row[0]!=scan["table_id"] for row in scan_rows): fail(f"{source}: direct scan table mismatch")
        direct.extend((scan["range_id"],)+row for row in scan_rows)
    sql = rows(r["sql_union_rows"], False, "sql")
    projected = sorted((table, rowid, seq, checksum) for _range, table, rowid, seq, checksum in direct)
    if oracle != projected or oracle != sql or len(projected)!=len(set(projected)): fail(f"{source}: oracle/direct/SQL row equality failed")
    if len(oracle) != r["acknowledged_rows"] or not oracle: fail(f"{source}: acknowledged row count mismatch")
    for range_id, table, rowid, _seq, _checksum in direct:
        owner = hash_owner(table,rowid) if hash_mode else 0 if table == 50 and rowid < 10 else 2 if (table == 50 or table == 51 and rowid < 16) else 3 if table == 51 else None
        if owner is None or range_id != owner: fail(f"{source}: physical row ownership mismatch")

    source_markers = marker_rows(r["source_markers"], "source markers")
    left = marker_rows(r["left_markers"], "left markers")
    right = marker_rows(r["right_markers"], "right markers")
    if set(left) & set(right) or left + right != source_markers: fail(f"{source}: marker disjoint ordered union failed")
    if marker_digest(source_markers) != r["marker_response_digest"]: fail(f"{source}: marker digest mismatch")
    if (len(source_markers), len(left), len(right)) != (r["marker_count"], r["left_marker_count"], r["right_marker_count"]) or (len(source_markers), len(left), len(right)) != (1, 0, 1): fail(f"{source}: marker counts/partition mismatch")

    terminal=exact_object(r["terminal_operation_evidence"],{"manifest_key","covered_offset","barrier_offset","tail_sha256","marker_digest"},"terminal operation evidence")
    if not isinstance(terminal["manifest_key"],str) or not terminal["manifest_key"] or type(terminal["covered_offset"]) is not int or terminal["covered_offset"] < 0 or type(terminal["barrier_offset"]) is not int or terminal["barrier_offset"] <= 0 or any(not isinstance(terminal[key],str) or len(terminal[key])!=64 for key in ["tail_sha256","marker_digest"]): fail(f"{source}: malformed terminal operation evidence")
    seen_receipts: set[str] = set(); receipt_events=[]
    marker_response = None
    for proof in r["authenticated_receipts"]:
        proof = exact_object(proof, {"sequence", "timestamp_ms", "operation", "endpoint", "range_id", "generation", "operation_id", "request", "response", "request_sha256", "response_sha256", "replay_count"}, "receipt")
        if proof["operation"] not in RECEIPTS or not isinstance(proof["endpoint"], str) or not proof["endpoint"] or proof["operation_id"] != r["operation_id"] or any(type(proof[key]) is not int for key in ["sequence","timestamp_ms","range_id","generation","replay_count"]) or proof["timestamp_ms"] <= 0 or proof["replay_count"] < 1: fail(f"{source}: malformed receipt identity/count")
        if digest(compact(proof["request"])) != proof["request_sha256"] or digest(compact(proof["response"])) != proof["response_sha256"]: fail(f"{source}: receipt hash mismatch")
        request = proof["request"]
        if request.get("operation_id") != r["operation_id"] or request.get("range_id") != proof["range_id"] or request.get("generation") != proof["generation"]: fail(f"{source}: receipt request identity mismatch")
        if request.get("operation",{}).get("operation") != REQUEST_KIND[proof["operation"]] or proof["response"].get("result") not in RESPONSE_KINDS[proof["operation"]]: fail(f"{source}: receipt operation/response kind mismatch")
        seen_receipts.add(proof["operation"])
        receipt_events.append(proof)
        if proof["operation"] == "markers": marker_response = proof["response"]
    if seen_receipts != RECEIPTS: fail(f"{source}: authenticated receipt operation set differs")
    if [p["sequence"] for p in receipt_events] != sorted(p["sequence"] for p in receipt_events) or [p["timestamp_ms"] for p in receipt_events] != sorted(p["timestamp_ms"] for p in receipt_events): fail(f"{source}: receipt observations are not ordered")
    first = {name:min(p["sequence"] for p in receipt_events if p["operation"]==name) for name in RECEIPTS}
    if [first[name] for name in RECEIPT_ORDER] != sorted(first.values()): fail(f"{source}: receipt phase order differs")
    replay_counts={}
    for p in receipt_events: replay_counts[(p["endpoint"],p["request_sha256"],p["response_sha256"])]=replay_counts.get((p["endpoint"],p["request_sha256"],p["response_sha256"]),0)+1
    if any(p["replay_count"] != replay_counts[(p["endpoint"],p["request_sha256"],p["response_sha256"])] for p in receipt_events): fail(f"{source}: receipt replay count mismatch")
    expectations={proof.get("operation"):proof for proof in r["journal_receipt_expectations"] if isinstance(proof,dict)}
    if set(expectations)!=RECEIPTS or len(expectations)!=len(r["journal_receipt_expectations"]): fail(f"{source}: journal receipt expectation set differs")
    for name in RECEIPTS:
        group=[proof for proof in receipt_events if proof["operation"]==name]
        expected=exact_object(expectations[name],{"operation","tenant","endpoint","range_id","generation","operation_id","request","request_sha256","expected_response_kind"},"journal receipt expectation")
        if expected["tenant"]!=r["tenant_id"] or expected["range_id"]!=1 or expected["generation"]!=0 or expected["operation_id"]!=r["operation_id"] or not isinstance(expected["endpoint"],str) or not expected["endpoint"]: fail(f"{source}: journal expectation source identity differs")
        if digest(compact(expected["request"]))!=expected["request_sha256"] or expected["request"].get("tenant")!=expected["tenant"] or expected["request"].get("range_id")!=1 or expected["request"].get("generation")!=0 or expected["request"].get("operation_id")!=r["operation_id"] or expected["request"].get("operation",{}).get("operation")!=REQUEST_KIND[name]: fail(f"{source}: journal expectation request differs")
        if any(proof["endpoint"]!=expected["endpoint"] or proof["range_id"]!=1 or proof["generation"]!=0 or proof["operation_id"]!=r["operation_id"] or proof["request_sha256"]!=expected["request_sha256"] or proof["request"]!=expected["request"] for proof in group): fail(f"{source}: authenticated replay differs from journal expectation")
        if name == "pause":
            offsets = [proof["response"].get("barrier_offset") for proof in group]
            if any(type(offset) is not int or offset <= 0 for offset in offsets) or offsets != sorted(offsets): fail(f"{source}: pause replay barrier offsets decrease or are invalid")
            if offsets[-1]!=terminal["barrier_offset"]: fail(f"{source}: pause replay does not end at durable barrier")
        elif name == "checkpoint":
            response=group[-1]["response"]
            if response.get("manifest_key")!=terminal["manifest_key"] or response.get("covered_offset")!=terminal["covered_offset"] or len({proof["response_sha256"] for proof in group})!=1: fail(f"{source}: checkpoint replay differs from durable evidence")
        elif name == "stage":
            tails=[proof["response"].get("tail_sha256") for proof in group]
            if any(not isinstance(tail,str) or len(tail)!=64 for tail in tails) or tails[-1]!=terminal["tail_sha256"]: fail(f"{source}: stage replay does not end at durable tail")
        elif name == "markers":
            if any(proof["response"].get("digest")!=terminal["marker_digest"] for proof in group) or len({proof["response_sha256"] for proof in group})!=1: fail(f"{source}: marker replay differs from durable evidence")
        allowed=RESPONSE_KINDS[name]
        expected_kind=expected["expected_response_kind"]
        if expected_kind=="applied_or_already_applied":
            if any(proof["response"].get("result") not in allowed for proof in group): fail(f"{source}: replay response kind differs")
        elif any(proof["response"].get("result")!=expected_kind for proof in group): fail(f"{source}: replay response kind differs")
    if not isinstance(marker_response, dict) or marker_response.get("digest") != r["marker_response_digest"]: fail(f"{source}: marker response identity missing")
    wire = lambda items: [(item["transaction_id"], item["key"]["table_id"], item["key"].get("bucket"), item["key"]["rowid"]) for item in items]
    if wire(marker_response.get("markers", [])) != source_markers or wire(marker_response.get("left_markers", [])) != left or wire(marker_response.get("right_markers", [])) != right: fail(f"{source}: marker response partitions differ")

    predecessor = f"__gres_wal.{r['tenant_id']}.r1"
    attempts = r["delete_attempts"]
    for attempt in attempts:
        attempt = exact_object(attempt, {"targets", "outcome"}, "delete attempt")
        if attempt["targets"] != [predecessor] or attempt["outcome"] not in {"deleted", "deleted_ack_lost"}: fail(f"{source}: delete target/outcome mismatch")
    if len(attempts) != r["delete_count"] or len(attempts) != 1 or r["unrelated_delete_attempted"]: fail(f"{source}: delete arithmetic/unrelated flag mismatch")

    if r["old_source_pid"] != r["old_pid"] or r["new_source_pid"] != r["new_pid"] or r["old_source_process_group"]!=r["old_source_pid"] or r["new_source_process_group"]!=r["new_source_pid"] or min(r["old_pid"], r["new_pid"], r["workload_process_group"]) <= 0 or r["old_pid"] == r["new_pid"]: fail(f"{source}: process identities invalid")
    if not r["new_source_pid_alive_at_verification"] or r["old_source_pid_alive"] or r["new_source_pid_alive"] or r["old_source_process_group_alive"] or r["new_source_process_group_alive"] or r["workload_process_group_alive"] or not r["workload_process_reaped"]: fail(f"{source}: terminal process cleanup invalid")
    if r["operation_revision"] <= 0 or r["operation_attempts"] < 1 or r["tenant_record_version"] != r["source_record_version"] + 2 or r["retirement_source_generation"] != 0 or r["retirement_successor_generations"] != [[0, 0], [2, 1], [3, 1]]: fail(f"{source}: journal/tenant/retirement versions invalid")
    if r["completed_phase"]!="completed" or r["pre_kill_predicate"]!=expected_predicate(case): fail(f"{source}: completed/pre-kill state mismatch")
    if not (r["marker_response_digest"]==r["operation_marker_digest"]==r["retirement_marker_digest"]==terminal["marker_digest"]): fail(f"{source}: marker digest chain differs")
    expected_layout=([{"range_id":0,"end_table_id":50,"end_bucket":4,"end_rowid":0,"endpoint":r["coordinator_endpoint"],"wal_generation":0},{"range_id":2,"end_table_id":50,"end_bucket":8,"end_rowid":0,"endpoint":r["left_endpoint"],"wal_generation":1},{"range_id":3,"end_table_id":None,"end_rowid":None,"endpoint":r["right_endpoint"],"wal_generation":1}] if hash_mode else [{"range_id":0,"end_table_id":50,"end_rowid":10,"endpoint":r["coordinator_endpoint"],"wal_generation":0},{"range_id":2,"end_table_id":51,"end_rowid":16,"endpoint":r["left_endpoint"],"wal_generation":1},{"range_id":3,"end_table_id":None,"end_rowid":None,"endpoint":r["right_endpoint"],"wal_generation":1}])
    layout=[]
    for entry in r["terminal_layout"]:
        keys=set(expected_layout[len(layout)])
        entry=exact_object(entry,keys,"terminal layout"); layout.append(entry)
    if layout!=expected_layout: fail(f"{source}: terminal r0/r2/r3 layout differs")

    bound = BOUNDS[family]
    if r["recovered_acknowledgements"] < 1 or r["max_ack_gap_bound_ms"] != bound or not 0 < r["max_ack_gap_ms"] <= bound or r["operation_bound_ms"] != 240_000 or not 0 < r["operation_elapsed_ms"] < 240_000 or not 0 < r["kill_ms"] <= r["restart_ms"] or r["publication_ms"] <= 0: fail(f"{source}: timing/count bounds invalid")
    if not all(r[key] for key in {"post_publication_r2_ack", "post_publication_r3_ack", "predecessor_topic_absent", "sentinel_topic_present"}): fail(f"{source}: terminal summary invariant false")
    if r["left_endpoint"] == r["right_endpoint"] or (r["left_wal_generation"], r["right_wal_generation"]) != (1, 1): fail(f"{source}: successor identity invalid")
    expected_topics = sorted({f"__gres_wal.{r['tenant_id']}.r0", f"__gres_wal.{r['tenant_id']}.r2.g0000000001", f"__gres_wal.{r['tenant_id']}.r3.g0000000001", r["sentinel_topic"]})
    if not r["sentinel_topic"].startswith("g8-sentinel-") or r["topology_topics"] != expected_topics: fail(f"{source}: exact topology topics differ")

def expected_hash_marker(case: str) -> tuple[int,int,int,int]:
    late={"initiated_before_running_cas","checkpoint_receipt_before_journal_cas","checkpointed_after_journal_cas"}
    return (1025,52,2,1026) if case in late else (1,52,2,2)

def validate_hash_evidence(value: Any, record: dict[str, Any], source: str) -> None:
    evidence=exact_object(value,HASH_EVIDENCE_KEYS,"hash evidence")
    if marker_rows(record["source_markers"], "hash source markers") != [expected_hash_marker(record["case"])]: fail(f"{source}: hash marker physical identity differs")
    algorithm=exact_object(evidence["algorithm"],{"name","offset_basis","prime","bucket_count"},"hash algorithm")
    if algorithm!={"name":"fnv1a64-int4-be","offset_basis":FNV_OFFSET,"prime":FNV_PRIME,"bucket_count":16}: fail(f"{source}: hash algorithm differs")
    boundary=exact_object(evidence["boundary"],{"table_id","bucket","rowid","request_bucket","receipt_bucket"},"hash boundary")
    if boundary!={"table_id":50,"bucket":8,"rowid":0,"request_bucket":8,"receipt_bucket":8}: fail(f"{source}: hash boundary differs")
    layout=record["terminal_layout"]
    if not isinstance(layout,list) or [entry.get("range_id") for entry in layout]!=[0,2,3]: fail(f"{source}: hash terminal range identities differ")
    boundary_entries=[entry for entry in layout if entry.get("end_table_id")==50 and entry.get("end_bucket")==8 and entry.get("end_rowid")==0]
    if len(boundary_entries)!=1 or [entry.get("wal_generation") for entry in layout]!=[0,1,1]: fail(f"{source}: hash terminal boundary/generations differ")
    corpus=[]
    for item in evidence["corpus"]:
        item=exact_object(item,{"logical_id","bytes_hex","bucket"},"hash corpus")
        if type(item["logical_id"]) is not int or item["bytes_hex"]!=struct.pack(">i",item["logical_id"]).hex() or item["bucket"]!=hash_bucket(bytes.fromhex(item["bytes_hex"]),16): fail(f"{source}: pinned hash corpus differs")
        corpus.append((item["logical_id"],item["bucket"]))
    if corpus!=[(logical_id,hash_bucket(struct.pack(">i",logical_id),16)) for logical_id in range(16)] or len({bucket for _,bucket in corpus})!=16: fail(f"{source}: pinned hash corpus is not the 0..15 bijection")
    snapshots=evidence["snapshots"]
    if not isinstance(snapshots,list): fail(f"{source}: snapshots must be list")
    if [(item.get("stage"),item.get("range_id")) for item in snapshots] != [(stage,range_id) for stage in ["before","after"] for range_id in range(4)]: fail(f"{source}: r0/r1/r2/r3 before/after coverage differs")
    decoded=[]
    for snapshot in snapshots:
        snapshot=exact_object(snapshot,{"stage","range_id","generation","sample_offset","records"},"hash snapshot")
        if snapshot["stage"] not in {"before","after"} or type(snapshot["range_id"]) is not int or type(snapshot["generation"]) is not int or type(snapshot["sample_offset"]) is not int or not isinstance(snapshot["records"],list): fail(f"{source}: malformed hash snapshot provenance")
        for item in snapshot["records"]:
            item=exact_object(item,{"raw_key_hex","raw_value_hex","source_offset","source_revision","corpus","summary"},"hash raw record")
            if type(item["source_offset"]) is not int or type(item["source_revision"]) is not int or item["source_offset"]<0 or item["source_revision"]<1 or type(item["corpus"]) is not bool: fail(f"{source}: malformed raw provenance")
            summary=decode_hash_row(item["raw_key_hex"],item["raw_value_hex"],"hash raw record")
            if item["summary"]!=summary: fail(f"{source}: raw row summary differs")
            decoded.append((snapshot["stage"],snapshot["range_id"],item["corpus"],summary))
    after=[summary for stage,_range,_corpus,summary in decoded if stage=="after" and summary["state"]=="committed"]
    if len({(row["table_id"],row["rowid"],row["version"]) for row in after})!=len(after): fail(f"{source}: raw hash rows are not unique")
    latest={}
    for stage,_range,corpus,row in decoded:
        if stage!="after" or row["state"]!="committed": continue
        key=(row["table_id"],row["rowid"])
        if key not in latest or latest[key][1]["version"]<row["version"]: latest[key]=(corpus,row)
    folds=exact_object(evidence["folds"],{"left_corpus","right_corpus","raw_after_sha256","sql_sha256","ack_sha256"},"hash folds")
    corpus_after=[summary for corpus,summary in latest.values() if corpus and summary["table_id"]==50]
    left=sum(row["bucket"]<8 for row in corpus_after); right=sum(row["bucket"]>=8 for row in corpus_after)
    if (left,right)!=(8,8) or (folds["left_corpus"],folds["right_corpus"])!=(8,8): fail(f"{source}: bucket-8 corpus fold differs")
    raw_projection=sorted((row["table_id"],row["logical_id"],row["rowid"],row["seq"],row["checksum"]) for _corpus,row in latest.values() if row["seq"] is not None)
    sql=[]
    for item in evidence["sql_rows"]:
        item=exact_object(item,{"table_id","logical_id","rowid","seq","checksum"},"hash SQL row")
        sql.append(tuple(item[key] for key in ["table_id","logical_id","rowid","seq","checksum"]))
    if sql!=sorted(sql) or sql!=raw_projection: fail(f"{source}: SQL/raw hash equality differs")
    ack=sorted((event["table_id"],event["rowid"],event["seq"],event["checksum"]) for event in record["payload_events"] if event["kind"] in {"ack","recovered_ack"})
    raw_ack=sorted((table,logical,seq,checksum) for table,logical,_rowid,seq,checksum in raw_projection)
    if ack!=raw_ack: fail(f"{source}: ACK/raw logical equality differs")
    encoded=lambda value:digest(compact(value))
    if folds["raw_after_sha256"]!=encoded(raw_projection) or folds["sql_sha256"]!=encoded(sql) or folds["ack_sha256"]!=encoded(ack): fail(f"{source}: physical/SQL/ACK fold digest differs")
    decisions=[]
    for item in evidence["transactions"]:
        item=exact_object(item,{"raw_key_hex","raw_value_hex","source_offset","source_revision","summary"},"hash transaction")
        if type(item["source_offset"]) is not int or type(item["source_revision"]) is not int or item["source_revision"]<1: fail(f"{source}: malformed transaction provenance")
        summary=decode_txd2(item["raw_key_hex"],item["raw_value_hex"],"hash transaction")
        normalized={**summary,"operations":[list(operation) for operation in summary["operations"]]}
        if item["summary"]!=normalized: fail(f"{source}: raw transaction summary differs")
        if summary["participants"]!=[0,1] or summary["prepared"]!=[0,1] or {operation[2] for operation in summary["operations"]}!={0,8} or len(summary["operations"])!=2: fail(f"{source}: hash transaction participant/bucket partition differs")
        decisions.append(summary["decision"])
    if sorted(decisions)!=["aborted","committed"] or "pending" in decisions: fail(f"{source}: terminal transaction decisions differ")

def validate_record(r: dict[str, Any], family: str, case: str, source: str) -> None:
    if r.get("schema_version")==2:
        if "hash_evidence" in r: fail(f"{source}: schema-v2 cannot carry hash evidence")
        validate_record_v2(r,family,case,source); return
    if r.get("schema_version")!=3 or set(r)!=KEYS|{"hash_evidence"}: fail(f"{source}: schema-v3 keys differ")
    base=copy.deepcopy(r); evidence=base.pop("hash_evidence"); base["schema_version"]=2
    validate_record_v2(base,family,case,source,hash_mode=True)
    validate_hash_evidence(evidence,r,source)

def require_family(family: str) -> None:
    if family not in EXPECTED: fail(f"unknown family {family!r}")
def validate_file(family: str, case: str, path: Path) -> dict[str, Any]:
    require_family(family); record = load(path); validate_record(record, family, case, str(path)); return record
def validate_family(family: str, directory: Path) -> list[dict[str, Any]]:
    require_family(family)
    expected = {f"{case}.json" for case in EXPECTED[family]}; actual = {path.name for path in directory.glob("*.json")} if directory.is_dir() else set()
    if actual != expected: fail(f"{directory}: file set differs")
    records = [validate_file(family, case, directory/f"{case}.json") for case in EXPECTED[family]]
    require_unique(records, str(directory)); return records
def require_unique(records: list[dict[str, Any]], label: str) -> None:
    for key in ["tenant_id", "operation_id", "sentinel_topic", "evidence_id"]:
        if len({record[key] for record in records}) != len(records): fail(f"{label}: duplicate {key}")
def validate_matrix(directory: Path) -> list[dict[str, Any]]:
    records = [record for family in EXPECTED for record in validate_family(family, directory/family)]
    require_unique(records, str(directory)); return records

def synthetic(family: str, case: str, index: int) -> dict[str, Any]:
    tenant=f"tg8synthetic-{family}-{index}"; operation=f"op-{family}-{index}"; sentinel=f"g8-sentinel-{family}-{index}"
    row={"table_id":50,"rowid":10,"seq":index+1,"checksum":f"c{index}"}; marker={"transaction_id":700,"table_id":52,"rowid":1}; wire={"transaction_id":700,"key":{"table_id":52,"rowid":1}}
    proofs=[]
    for sequence,name in enumerate(RECEIPT_ORDER):
        request={"tenant":tenant,"range_id":1,"generation":0,"operation_id":operation,"operation":{"operation":REQUEST_KIND[name]}}
        response={"result":next(iter(RESPONSE_KINDS[name]))}
        if name=="pause": response={"result":"paused","barrier_offset":1}
        if name=="markers": response={"result":"markers","markers":[wire],"left_markers":[],"right_markers":[wire],"digest":marker_digest([(700,52,1)])}
        proofs.append({"sequence":sequence,"timestamp_ms":sequence+1,"operation":name,"endpoint":"127.0.0.1:9000","range_id":1,"generation":0,"operation_id":operation,"request":request,"response":response,"request_sha256":digest(compact(request)),"response_sha256":digest(compact(response)),"replay_count":1})
    expectations=[]
    for name in sorted(RECEIPTS):
        proof=next(item for item in proofs if item["operation"]==name)
        expectations.append({"operation":name,"tenant":tenant,"endpoint":proof["endpoint"],"range_id":1,"generation":0,"operation_id":operation,"request":copy.deepcopy(proof["request"]),"request_sha256":proof["request_sha256"],"expected_response_kind":"applied_or_already_applied" if name in {"prologue","retire"} else next(iter(RESPONSE_KINDS[name]))})
    result={key:0 for key in INTS}; result.update({key:"x" for key in STRINGS}); result.update({key:False for key in BOOLS}); result.update({key:[] for key in LISTS})
    result.update({"schema_version":1,"family":family,"case":case,"tenant_id":tenant,"operation_id":operation,"evidence_id":digest(f"{family}\0{case}\0{tenant}\0{operation}".encode()),"acknowledged_rows":1,"recovered_acknowledgements":1,"max_ack_gap_ms":1,"max_ack_gap_bound_ms":BOUNDS[family],"operation_elapsed_ms":2,"operation_bound_ms":240000,"marker_count":1,"right_marker_count":1,"delete_count":1,"old_pid":index*3+1,"new_pid":index*3+2,"old_source_pid":index*3+1,"new_source_pid":index*3+2,"workload_process_group":index*3+3,"kill_ms":1,"restart_ms":2,"publication_ms":3,"left_wal_generation":1,"right_wal_generation":1,"operation_revision":9,"operation_attempts":2,"tenant_record_version":6,"source_record_version":4,"retirement_successor_generations":[[0,0],[2,1],[3,1]],"sentinel_topic":sentinel,"left_endpoint":"left","right_endpoint":"right","marker_response_digest":marker_digest([(700,52,1)]),"post_publication_r2_ack":True,"post_publication_r3_ack":True,"predecessor_topic_absent":True,"sentinel_topic_present":True,"workload_process_reaped":True,"new_source_pid_alive_at_verification":True,"topology_topics":sorted([f"__gres_wal.{tenant}.r0",f"__gres_wal.{tenant}.r2.g0000000001",f"__gres_wal.{tenant}.r3.g0000000001",sentinel]),"reopened_oracle_rows":[row],"direct_physical_rows":[{"range_id":2,**row}],"sql_union_rows":[row],"source_markers":[marker],"right_markers":[marker],"authenticated_receipts":proofs,"delete_attempts":[{"targets":[f"__gres_wal.{tenant}.r1"],"outcome":"deleted"}]})
    synthetic_rows=[{"table_id":50,"rowid":10,"seq":1,"checksum":"pre"},{"table_id":50,"rowid":16,"seq":2,"checksum":"left"},{"table_id":51,"rowid":16,"seq":3,"checksum":"right"}]
    payload=[]
    for item,timestamp,kind in [(synthetic_rows[0],10,"ack"),(synthetic_rows[1],50,"recovered_ack"),(synthetic_rows[2],51,"ack")]:
        payload.append({"kind":"attempt","provenance":"workload","table_id":item["table_id"],"rowid":None,"seq":item["seq"],"checksum":item["checksum"],"timestamp_ms":timestamp-1})
        if kind=="recovered_ack":
            payload.append({"kind":"retry","provenance":"workload","table_id":item["table_id"],"rowid":None,"seq":item["seq"],"checksum":item["checksum"],"timestamp_ms":timestamp-1})
        payload.append({"kind":kind,"provenance":"workload","table_id":item["table_id"],"rowid":item["rowid"],"seq":item["seq"],"checksum":item["checksum"],"timestamp_ms":timestamp})
    empty=lambda range_id,table_id:{"range_id":range_id,"table_id":table_id,"rows":[]}
    scans=[empty(0,50),empty(0,51),{"range_id":2,"table_id":50,"rows":synthetic_rows[:2]},empty(2,51),empty(3,50),{"range_id":3,"table_id":51,"rows":[synthetic_rows[2]]}]
    result.update({"acknowledged_rows":3,"max_ack_gap_ms":40,"kill_ms":20,"restart_ms":30,"publication_ms":40,"coordinator_endpoint":"coordinator","old_source_process_group":result["old_source_pid"],"new_source_process_group":result["new_source_pid"],"payload_events":payload,"reopened_oracle_rows":synthetic_rows,"direct_physical_rows":scans,"sql_union_rows":synthetic_rows,"journal_receipt_expectations":expectations,"completed_phase":"completed","pre_kill_predicate":expected_predicate(case),"operation_marker_digest":result["marker_response_digest"],"retirement_marker_digest":result["marker_response_digest"],"terminal_layout":[{"range_id":0,"end_table_id":50,"end_rowid":10,"endpoint":"coordinator","wal_generation":0},{"range_id":2,"end_table_id":51,"end_rowid":16,"endpoint":"left","wal_generation":1},{"range_id":3,"end_table_id":None,"end_rowid":None,"endpoint":"right","wal_generation":1}]})
    result["schema_version"]=2
    result["terminal_operation_evidence"]={"manifest_key":"manifest","covered_offset":1,"barrier_offset":1,"tail_sha256":"1"*64,"marker_digest":result["marker_response_digest"]}
    checkpoint=next(item for item in result["authenticated_receipts"] if item["operation"]=="checkpoint"); checkpoint["response"].update(manifest_key="manifest",covered_offset=1); checkpoint["response_sha256"]=digest(compact(checkpoint["response"]))
    stage=next(item for item in result["authenticated_receipts"] if item["operation"]=="stage"); stage["response"]["tail_sha256"]="1"*64; stage["response_sha256"]=digest(compact(stage["response"]))
    pause=next(item for item in result["authenticated_receipts"] if item["operation"]=="pause"); pause["response"]["barrier_offset"]=1; pause["response_sha256"]=digest(compact(pause["response"]))
    return result

def encode_hash_row(table_id:int,logical_id:int,rowid:int,version:int,start_ts:int,commit_ts:int,seq:int|None=None,checksum:str|None=None,state:int=2)->tuple[str,str,dict[str,Any]]:
    bucket=hash_bucket(struct.pack(">i",logical_id),16)
    key=struct.pack(">IIIQQ",table_id,1,bucket,rowid,version)
    row=bytearray([1,2]); row.extend(struct.pack(">i",logical_id))
    if seq is not None:
        row.append(2); row.extend(struct.pack(">i",seq)); raw=checksum.encode() if checksum is not None else b""
        row.append(4); row.extend(struct.pack(">I",len(raw))); row.extend(raw)
    value=bytes([2,state])+struct.pack(">QQ",start_ts,commit_ts if state==2 else 0)+row
    return key.hex(),value.hex(),decode_hash_row(key.hex(),value.hex(),"synthetic hash row")

def encode_txd2(start_ts:int,decision:str)->tuple[str,str,dict[str,Any]]:
    key=b"\0\0\0\0meta/ts_txn/"+struct.pack(">Q",start_ts)
    operations=[(0,50,0,100,False),(1,50,8,200,False)]
    value=bytearray(b"TXD2"+struct.pack(">QQI",start_ts+100,2,2)+struct.pack(">II",0,1)+struct.pack(">I",2)+struct.pack(">II",0,1)+struct.pack(">I",2))
    for range_id,table_id,bucket,rowid,delete in operations:
        value.extend(struct.pack(">IIBIQB",range_id,table_id,1,bucket,rowid,delete))
    value.extend(b"\1" if decision=="aborted" else b"\2"+struct.pack(">Q",start_ts+1))
    summary=decode_txd2(key.hex(),value.hex(),"synthetic TXD2")
    summary={**summary,"operations":[list(operation) for operation in summary["operations"]]}
    return key.hex(),value.hex(),summary

def synthetic_v3(family:str,case:str,index:int)->dict[str,Any]:
    result=synthetic(family,case,index); result["schema_version"]=3
    transaction_id,table_id,bucket,rowid=expected_hash_marker(case)
    marker={"transaction_id":transaction_id,"table_id":table_id,"bucket":bucket,"rowid":rowid}
    wire={"transaction_id":transaction_id,"key":{"table_id":table_id,"bucket":bucket,"rowid":rowid}}
    marker_hash=marker_digest([(transaction_id,table_id,bucket,rowid)])
    result["source_markers"]=[marker]; result["left_markers"]=[]; result["right_markers"]=[marker]
    result["marker_response_digest"]=marker_hash; result["operation_marker_digest"]=marker_hash; result["retirement_marker_digest"]=marker_hash
    result["terminal_operation_evidence"]["marker_digest"]=marker_hash
    marker_proof=next(item for item in result["authenticated_receipts"] if item["operation"]=="markers")
    marker_proof["response"]={"result":"markers","markers":[wire],"left_markers":[],"right_markers":[wire],"digest":marker_hash}
    marker_proof["response_sha256"]=digest(compact(marker_proof["response"]))
    result["terminal_layout"]=[
        {"range_id":0,"end_table_id":50,"end_bucket":4,"end_rowid":0,"endpoint":result["coordinator_endpoint"],"wal_generation":0},
        {"range_id":2,"end_table_id":50,"end_bucket":8,"end_rowid":0,"endpoint":result["left_endpoint"],"wal_generation":1},
        {"range_id":3,"end_table_id":None,"end_rowid":None,"endpoint":result["right_endpoint"],"wal_generation":1},
    ]
    scans={(range_id,table_id):[] for range_id in [0,2,3] for table_id in [50,51]}
    for row in result["reopened_oracle_rows"]:
        scans[(hash_owner(row["table_id"],row["rowid"]),row["table_id"])].append(copy.deepcopy(row))
    result["direct_physical_rows"]=[{"range_id":range_id,"table_id":table_id,"rows":scans[(range_id,table_id)]} for range_id,table_id in [(0,50),(0,51),(2,50),(2,51),(3,50),(3,51)]]
    records=[]
    for logical_id in range(16):
        key,value,summary=encode_hash_row(50,logical_id,1000+logical_id,2000+logical_id,3000+logical_id,4000+logical_id)
        records.append({"raw_key_hex":key,"raw_value_hex":value,"source_offset":logical_id,"source_revision":logical_id+1,"corpus":True,"summary":summary})
    physical_sql=[]
    for offset,row in enumerate(result["reopened_oracle_rows"],100):
        key,value,summary=encode_hash_row(row["table_id"],row["rowid"],9000+offset,8000+offset,7000+offset,7100+offset,row["seq"],row["checksum"])
        records.append({"raw_key_hex":key,"raw_value_hex":value,"source_offset":offset,"source_revision":offset+1,"corpus":False,"summary":summary})
        physical_sql.append({"table_id":summary["table_id"],"logical_id":summary["logical_id"],"rowid":summary["rowid"],"seq":summary["seq"],"checksum":summary["checksum"]})
    snapshots=[]
    for stage in ["before","after"]:
        for range_id in range(4):
            assigned=[] if stage=="before" else [item for item in records if item["summary"]["bucket"]%4==range_id]
            snapshots.append({"stage":stage,"range_id":range_id,"generation":0 if range_id<2 else 1,"sample_offset":999,"records":copy.deepcopy(assigned)})
    transactions=[]
    for offset,decision in enumerate(["committed","aborted"],500):
        key,value,summary=encode_txd2(offset,decision)
        transactions.append({"raw_key_hex":key,"raw_value_hex":value,"source_offset":offset,"source_revision":offset+1,"summary":summary})
    raw_projection=sorted((item["table_id"],item["logical_id"],item["rowid"],item["seq"],item["checksum"]) for item in physical_sql)
    ack=sorted((event["table_id"],event["rowid"],event["seq"],event["checksum"]) for event in result["payload_events"] if event["kind"] in {"ack","recovered_ack"})
    result["hash_evidence"]={
        "algorithm":{"name":"fnv1a64-int4-be","offset_basis":FNV_OFFSET,"prime":FNV_PRIME,"bucket_count":16},
        "boundary":{"table_id":50,"bucket":8,"rowid":0,"request_bucket":8,"receipt_bucket":8},
        "corpus":[{"logical_id":logical_id,"bytes_hex":struct.pack(">i",logical_id).hex(),"bucket":hash_bucket(struct.pack(">i",logical_id),16)} for logical_id in range(16)],
        "snapshots":snapshots,"sql_rows":sorted(physical_sql,key=lambda item:tuple(item[key] for key in ["table_id","logical_id","rowid","seq","checksum"])),"transactions":transactions,
        "folds":{"left_corpus":8,"right_corpus":8,"raw_after_sha256":digest(compact(raw_projection)),"sql_sha256":digest(compact(sorted(tuple(item[key] for key in ["table_id","logical_id","rowid","seq","checksum"]) for item in physical_sql))),"ack_sha256":digest(compact(ack))},
    }
    return result

def expect_bad(record: dict[str,Any], family:str, case:str, mutate:Callable[[dict[str,Any]],None], label:str) -> None:
    changed=copy.deepcopy(record); mutate(changed)
    try: validate_record(changed,family,case,label)
    except ValidationError: return
    fail(f"negative unexpectedly passed: {label}")
def mutate_expectation(record: dict[str,Any], operation: str, field: str, value: Any) -> None:
    expectation=next(item for item in record["journal_receipt_expectations"] if item["operation"]==operation)
    if field in {"tenant","endpoint","range_id","generation"}:
        expectation[field]=value
        if field!="endpoint": expectation["request"][field]=value
    else:
        expectation["request"]["operation"][field]=value
    expectation["request_sha256"]=digest(compact(expectation["request"]))
def append_replay(record: dict[str,Any], operation: str, response: dict[str,Any]) -> None:
    original=next(item for item in record["authenticated_receipts"] if item["operation"]==operation)
    replay=copy.deepcopy(original); replay["sequence"]=max(item["sequence"] for item in record["authenticated_receipts"])+1; replay["timestamp_ms"]=max(item["timestamp_ms"] for item in record["authenticated_receipts"])+1
    replay["response"]=response; replay["response_sha256"]=digest(compact(response)); replay["replay_count"]=1
    record["authenticated_receipts"].append(replay)
def mutate_last_response(record: dict[str,Any], operation: str, response: dict[str,Any]) -> None:
    replay=max((item for item in record["authenticated_receipts"] if item["operation"]==operation),key=lambda item:item["sequence"])
    replay["response"]=response; replay["response_sha256"]=digest(compact(response))
def self_test() -> None:
    with tempfile.TemporaryDirectory() as raw:
        root=Path(raw); all_records=[]
        for family,cases in EXPECTED.items():
            d=root/family; d.mkdir()
            for i,case in enumerate(cases):
                record=synthetic(family,case,100*list(EXPECTED).index(family)+i); (d/f"{case}.json").write_text(json.dumps(record)); all_records.append(record)
            validate_family(family,d)
        validate_matrix(root)
        family="publication"; case=EXPECTED[family][0]; good=synthetic(family,case,999)
        pause=next(item for item in good["authenticated_receipts"] if item["operation"]=="pause")
        pause["response"]={"result":"paused","barrier_offset":1}; pause["response_sha256"]=digest(compact(pause["response"])); append_replay(good,"pause",{"result":"paused","barrier_offset":2})
        good["terminal_operation_evidence"]["barrier_offset"]=2
        append_replay(good,"stage",{"result":"staged","tail_sha256":"2"*64}); good["terminal_operation_evidence"]["tail_sha256"]="2"*64
        for operation in ["prologue","retire"]:
            original=next(item for item in good["authenticated_receipts"] if item["operation"]==operation)["response"]["result"]
            append_replay(good,operation,{"result":"applied" if original=="already_applied" else "already_applied"})
        negatives=[
            ("evidence id",lambda r:r.__setitem__("evidence_id","0"*64)),("wrong family",lambda r:r.__setitem__("family","source_restore")),("wrong case",lambda r:r.__setitem__("case",EXPECTED[family][1])),("schema missing",lambda r:r.pop("delete_count")),("payload checksum",lambda r:r["payload_events"][1].__setitem__("checksum","wrong")),("recovered count",lambda r:r.__setitem__("recovered_acknowledgements",2)),("computed gap",lambda r:r.__setitem__("max_ack_gap_ms",39)),("pre-kill ack",lambda r:r.__setitem__("kill_ms",5)),("post-restart ack",lambda r:r.__setitem__("restart_ms",60)),("post-publication stream",lambda r:r.__setitem__("publication_ms",51)),("oracle mismatch",lambda r:r["reopened_oracle_rows"][0].__setitem__("seq",99)),("direct ownership",lambda r:r["direct_physical_rows"][0].__setitem__("range_id",3)),("direct scan missing",lambda r:r["direct_physical_rows"].pop()),("sql mismatch",lambda r:r["sql_union_rows"].clear()),("row count",lambda r:r.__setitem__("acknowledged_rows",2)),("marker overlap",lambda r:r.__setitem__("left_markers",r["right_markers"])),("marker order",lambda r:r.__setitem__("source_markers",r["source_markers"]*2)),("marker digest",lambda r:r.__setitem__("marker_response_digest","0"*64)),("marker response",lambda r:r["authenticated_receipts"][3]["response"].__setitem__("right_markers",[])),("receipt hash",lambda r:r["authenticated_receipts"][0].__setitem__("request_sha256","0"*64)),("receipt identity",lambda r:r["authenticated_receipts"][0].__setitem__("operation_id","wrong")),("receipt operation",lambda r:r["authenticated_receipts"][0].__setitem__("operation","pause")),("receipt response",lambda r:r["authenticated_receipts"][0]["response"].__setitem__("result","rejected")),("receipt order",lambda r:r["authenticated_receipts"][0].__setitem__("sequence",99)),("receipt timestamp",lambda r:r["authenticated_receipts"][0].__setitem__("timestamp_ms",99)),("receipt replay",lambda r:r["authenticated_receipts"][0].__setitem__("replay_count",0)),("receipt missing",lambda r:r["authenticated_receipts"].pop()),("expectation tenant",lambda r:mutate_expectation(r,"stage","tenant","wrong")),("expectation endpoint",lambda r:mutate_expectation(r,"stage","endpoint","wrong")),("expectation range",lambda r:mutate_expectation(r,"stage","range_id",2)),("expectation generation",lambda r:mutate_expectation(r,"stage","generation",1)),("expectation journal digest",lambda r:mutate_expectation(r,"stage","journal_digest","0"*64)),("delete target",lambda r:r["delete_attempts"][0].__setitem__("targets",["other"])),("delete outcome",lambda r:r["delete_attempts"][0].__setitem__("outcome","error")),("delete count",lambda r:r.__setitem__("delete_count",2)),("unrelated delete",lambda r:r.__setitem__("unrelated_delete_attempted",True)),("pid identity",lambda r:r.__setitem__("old_source_pid",999)),("pgid identity",lambda r:r.__setitem__("old_source_process_group",999)),("old pid alive",lambda r:r.__setitem__("old_source_pid_alive",True)),("old group alive",lambda r:r.__setitem__("old_source_process_group_alive",True)),("new pid dead at verify",lambda r:r.__setitem__("new_source_pid_alive_at_verification",False)),("new pid alive terminal",lambda r:r.__setitem__("new_source_pid_alive",True)),("new group alive terminal",lambda r:r.__setitem__("new_source_process_group_alive",True)),("workload alive",lambda r:r.__setitem__("workload_process_group_alive",True)),("completed phase",lambda r:r.__setitem__("completed_phase","retiring")),("pre-kill predicate",lambda r:r["pre_kill_predicate"].__setitem__("phase","wrong")),("operation digest",lambda r:r.__setitem__("operation_marker_digest","0"*64)),("retirement digest",lambda r:r.__setitem__("retirement_marker_digest","0"*64)),("terminal layout",lambda r:r["terminal_layout"][1].__setitem__("range_id",9)),("journal revision",lambda r:r.__setitem__("operation_revision",0)),("operation attempts",lambda r:r.__setitem__("operation_attempts",0)),("tenant version",lambda r:r.__setitem__("tenant_record_version",4)),("retirement source",lambda r:r.__setitem__("retirement_source_generation",1)),("retirement versions",lambda r:r.__setitem__("retirement_successor_generations",[[2,1],[3,2]])),("endpoint",lambda r:r.__setitem__("right_endpoint","left")),("generation",lambda r:r.__setitem__("right_wal_generation",2)),("topics",lambda r:r["topology_topics"].pop()),("sentinel",lambda r:r.__setitem__("sentinel_topic","bad")),("false invariant",lambda r:r.__setitem__("post_publication_r2_ack",False)),("wrong bound",lambda r:r.__setitem__("max_ack_gap_bound_ms",25000)),("schema extra",lambda r:r.__setitem__("extra",1)),("unknown payload kind",lambda r:r["payload_events"][0].__setitem__("kind","mystery")),("retry orphan",lambda r:r["payload_events"].insert(0,{"kind":"retry","provenance":"workload","table_id":50,"rowid":None,"seq":99,"checksum":"c","timestamp_ms":1})),("retry rowid",lambda r:next(event for event in r["payload_events"] if event["kind"]=="retry").__setitem__("rowid",5)),("retry checksum",lambda r:next(event for event in r["payload_events"] if event["kind"]=="retry").__setitem__("checksum","wrong"))]
        negatives=[(label,(lambda r:r["authenticated_receipts"].pop(0)) if label=="receipt missing" else mutation) for label,mutation in negatives]
        negatives.extend([
            ("pause replay decreasing",lambda r:mutate_last_response(r,"pause",{"result":"paused","barrier_offset":0})),
            ("pause replay changed result",lambda r:mutate_last_response(r,"pause",{"result":"rejected","barrier_offset":2})),
            ("prologue replay forbidden kind",lambda r:mutate_last_response(r,"prologue",{"result":"rejected"})),
            ("retire replay forbidden kind",lambda r:mutate_last_response(r,"retire",{"result":"rejected"})),
            ("stage replay invalid hash",lambda r:next(item for item in r["authenticated_receipts"] if item["operation"]=="stage").update(response={"result":"staged","tail_sha256":"bad"},response_sha256=digest(compact({"result":"staged","tail_sha256":"bad"})))),
            ("stage replay terminal mismatch",lambda r:r["terminal_operation_evidence"].__setitem__("tail_sha256","3"*64)),
        ])
        validate_record(good,family,case,"operation-specific replay positive")
        for label,mutation in negatives: expect_bad(good,family,case,mutation,label)
        hash_good=synthetic_v3(family,case,1001)
        validate_record(hash_good,family,case,"schema-v3 positive")
        first_record=lambda r:r["hash_evidence"]["snapshots"][4]["records"][0]
        hash_negatives=[
            ("hash missing logical id",lambda r:first_record(r)["summary"].pop("logical_id")),
            ("hash wrong logical id",lambda r:first_record(r)["summary"].__setitem__("logical_id",99)),
            ("hash wrong rowid",lambda r:first_record(r)["summary"].__setitem__("rowid",99)),
            ("hash wrong bucket",lambda r:first_record(r)["summary"].__setitem__("bucket",9)),
            ("hash wrong class",lambda r:first_record(r)["summary"].__setitem__("key_class","primary_version")),
            ("hash wrong raw key",lambda r:first_record(r).__setitem__("raw_key_hex","01"+first_record(r)["raw_key_hex"][2:])),
            ("hash wrong raw value",lambda r:first_record(r).__setitem__("raw_value_hex","00"+first_record(r)["raw_value_hex"][2:])),
            ("hash wrong fold",lambda r:r["hash_evidence"]["folds"].__setitem__("left_corpus",7)),
            ("hash wrong transaction state",lambda r:r["hash_evidence"]["transactions"][0].__setitem__("raw_value_hex",r["hash_evidence"]["transactions"][0]["raw_value_hex"][:-18]+"00")),
            ("hash missing provenance",lambda r:first_record(r).pop("source_revision")),
            ("hash bad provenance",lambda r:first_record(r).__setitem__("source_offset",-1)),
            ("hash wrong boundary",lambda r:r["hash_evidence"]["boundary"].__setitem__("receipt_bucket",7)),
            ("hash missing stage",lambda r:r["hash_evidence"]["snapshots"].pop()),
            ("hash wrong schema",lambda r:r.__setitem__("schema_version",2)),
        ]
        for label,mutation in hash_negatives: expect_bad(hash_good,family,case,mutation,label)
        cross=copy.deepcopy(good); cross["schema_version"]=3
        try: validate_record(cross,family,case,"v2-as-v3")
        except ValidationError: pass
        else: fail("schema-v2 record unexpectedly accepted as v3")
        cross=copy.deepcopy(hash_good); cross["schema_version"]=2
        try: validate_record(cross,family,case,"v3-as-v2")
        except ValidationError: pass
        else: fail("schema-v3 record unexpectedly accepted as v2")
        for key in ["tenant_id","operation_id","sentinel_topic","evidence_id"]:
            duplicate=copy.deepcopy(all_records); duplicate[-1][key]=duplicate[0][key]
            try: require_unique(duplicate,"cross-family")
            except ValidationError: pass
            else: fail(f"cross-family duplicate {key} unexpectedly passed")
        source_dir=root/"source_restore"; missing=source_dir/f"{EXPECTED['source_restore'][0]}.json"; saved=missing.read_text(); missing.unlink()
        try: validate_family("source_restore",source_dir)
        except ValidationError: pass
        else: fail("missing family case unexpectedly passed")
        missing.write_text(saved); extra=source_dir/"extra.json"; extra.write_text(saved)
        try: validate_family("source_restore",source_dir)
        except ValidationError: pass
        else: fail("extra family case unexpectedly passed")

def main() -> int:
    parser=argparse.ArgumentParser(); modes=parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--validate-file",nargs=3,metavar=("FAMILY","CASE","FILE")); modes.add_argument("--validate-family",nargs=2,metavar=("FAMILY","DIR")); modes.add_argument("--validate-matrix",metavar="DIR"); modes.add_argument("--self-test",action="store_true"); args=parser.parse_args()
    try:
        if args.self_test:self_test()
        elif args.validate_file:validate_file(args.validate_file[0],args.validate_file[1],Path(args.validate_file[2]))
        elif args.validate_family:validate_family(args.validate_family[0],Path(args.validate_family[1]))
        else:validate_matrix(Path(args.validate_matrix))
    except ValidationError as error: parser.error(str(error))
    return 0
if __name__=="__main__": raise SystemExit(main())
