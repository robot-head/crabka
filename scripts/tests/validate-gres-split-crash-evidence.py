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
BOUNDS = {"source_restore": 25_000, "publication": 15_000, "retirement_resume": 15_000}
INTS = {"schema_version", "acknowledged_rows", "recovered_acknowledgements", "max_ack_gap_ms", "max_ack_gap_bound_ms", "operation_elapsed_ms", "operation_bound_ms", "marker_count", "left_marker_count", "right_marker_count", "delete_count", "old_pid", "new_pid", "kill_ms", "restart_ms", "publication_ms", "left_wal_generation", "right_wal_generation", "old_source_pid", "new_source_pid", "workload_process_group", "operation_revision", "operation_attempts", "tenant_record_version", "source_record_version", "retirement_source_generation"}
STRINGS = {"evidence_id", "family", "case", "tenant_id", "operation_id", "sentinel_topic", "left_endpoint", "right_endpoint", "marker_response_digest"}
BOOLS = {"post_publication_r2_ack", "post_publication_r3_ack", "predecessor_topic_absent", "sentinel_topic_present", "workload_process_reaped", "unrelated_delete_attempted", "new_source_pid_alive_at_verification", "old_source_pid_alive", "new_source_pid_alive", "workload_process_group_alive"}
LISTS = {"topology_topics", "reopened_oracle_rows", "direct_physical_rows", "sql_union_rows", "source_markers", "left_markers", "right_markers", "authenticated_receipts", "delete_attempts", "retirement_successor_generations"}
KEYS = INTS | STRINGS | BOOLS | LISTS
RECEIPTS = {"checkpoint", "pause", "stage", "markers", "prologue", "retire"}
RECEIPT_ORDER = ["checkpoint", "pause", "stage", "markers", "prologue", "retire"]
REQUEST_KIND = {"checkpoint":"force_checkpoint", "pause":"pause_at_covered_offset", "stage":"stage_filtered_restore", "markers":"inherit_markers", "prologue":"successor_fence_prologue", "retire":"retire_predecessor"}
RESPONSE_KINDS = {"checkpoint":{"checkpoint"}, "pause":{"paused"}, "stage":{"staged"}, "markers":{"markers"}, "prologue":{"applied","already_applied"}, "retire":{"applied","already_applied"}}

class ValidationError(ValueError): pass
def fail(message: str) -> None: raise ValidationError(message)
def compact(value: Any) -> bytes: return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
def digest(value: bytes) -> str: return hashlib.sha256(value).hexdigest()

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

def marker_rows(value: Any, label: str) -> list[tuple[int, int, int]]:
    if not isinstance(value, list): fail(f"{label}: markers must be list")
    result = []
    for item in value:
        item = exact_object(item, {"transaction_id", "table_id", "rowid"}, label)
        if any(type(item[key]) is not int or item[key] < 0 for key in item): fail(f"{label}: malformed marker")
        result.append((item["transaction_id"], item["table_id"], item["rowid"]))
    if result != sorted(result) or len(result) != len(set(result)): fail(f"{label}: markers must be sorted and unique")
    return result

def marker_digest(markers: list[tuple[int, int, int]]) -> str:
    hasher = hashlib.sha256()
    for marker in markers:
        for number in marker: hasher.update(struct.pack(">Q", number))
    return hasher.hexdigest()

def validate_record(r: dict[str, Any], family: str, case: str, source: str) -> None:
    if set(r) != KEYS: fail(f"{source}: schema keys differ: missing={sorted(KEYS-set(r))} extra={sorted(set(r)-KEYS)}")
    for key in INTS:
        if type(r[key]) is not int: fail(f"{source}: {key} must be integer")
    for key in STRINGS:
        if not isinstance(r[key], str) or not r[key]: fail(f"{source}: {key} must be nonempty string")
    for key in BOOLS:
        if type(r[key]) is not bool: fail(f"{source}: {key} must be boolean")
    for key in LISTS:
        if not isinstance(r[key], list): fail(f"{source}: {key} must be list")
    if r["schema_version"] != 1 or r["family"] != family or r["case"] != case or case not in EXPECTED[family]: fail(f"{source}: schema/family/case mismatch")
    expected_evidence_id = digest(f"{family}\0{case}\0{r['tenant_id']}\0{r['operation_id']}".encode())
    if r["evidence_id"] != expected_evidence_id: fail(f"{source}: evidence_id mismatch")

    oracle = rows(r["reopened_oracle_rows"], False, "oracle")
    direct = rows(r["direct_physical_rows"], True, "direct")
    sql = rows(r["sql_union_rows"], False, "sql")
    projected = [(table, rowid, seq, checksum) for _range, table, rowid, seq, checksum in direct]
    if oracle != projected or oracle != sql: fail(f"{source}: oracle/direct/SQL row equality failed")
    if len(oracle) != r["acknowledged_rows"] or not oracle: fail(f"{source}: acknowledged row count mismatch")
    for range_id, table, rowid, _seq, _checksum in direct:
        owner = 0 if table == 50 and rowid < 10 else 2 if (table == 50 or table == 51 and rowid < 16) else 3 if table == 51 else None
        if owner is None or range_id != owner: fail(f"{source}: physical row ownership mismatch")

    source_markers = marker_rows(r["source_markers"], "source markers")
    left = marker_rows(r["left_markers"], "left markers")
    right = marker_rows(r["right_markers"], "right markers")
    if set(left) & set(right) or left + right != source_markers: fail(f"{source}: marker disjoint ordered union failed")
    if marker_digest(source_markers) != r["marker_response_digest"]: fail(f"{source}: marker digest mismatch")
    if (len(source_markers), len(left), len(right)) != (r["marker_count"], r["left_marker_count"], r["right_marker_count"]) or (len(source_markers), len(left), len(right)) != (1, 0, 1): fail(f"{source}: marker counts/partition mismatch")

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
    if not isinstance(marker_response, dict) or marker_response.get("digest") != r["marker_response_digest"]: fail(f"{source}: marker response identity missing")
    wire = lambda items: [(item["transaction_id"], item["key"]["table_id"], item["key"]["rowid"]) for item in items]
    if wire(marker_response.get("markers", [])) != source_markers or wire(marker_response.get("left_markers", [])) != left or wire(marker_response.get("right_markers", [])) != right: fail(f"{source}: marker response partitions differ")

    predecessor = f"__gres_wal.{r['tenant_id']}.r1"
    attempts = r["delete_attempts"]
    for attempt in attempts:
        attempt = exact_object(attempt, {"targets", "outcome"}, "delete attempt")
        if attempt["targets"] != [predecessor] or attempt["outcome"] not in {"deleted", "deleted_ack_lost"}: fail(f"{source}: delete target/outcome mismatch")
    if len(attempts) != r["delete_count"] or len(attempts) != 1 or r["unrelated_delete_attempted"]: fail(f"{source}: delete arithmetic/unrelated flag mismatch")

    if r["old_source_pid"] != r["old_pid"] or r["new_source_pid"] != r["new_pid"] or min(r["old_pid"], r["new_pid"], r["workload_process_group"]) <= 0 or r["old_pid"] == r["new_pid"]: fail(f"{source}: process identities invalid")
    if not r["new_source_pid_alive_at_verification"] or r["old_source_pid_alive"] or r["new_source_pid_alive"] or r["workload_process_group_alive"] or not r["workload_process_reaped"]: fail(f"{source}: terminal process cleanup invalid")
    if r["operation_revision"] <= 0 or r["operation_attempts"] < 1 or r["tenant_record_version"] != r["source_record_version"] + 2 or r["retirement_source_generation"] != 0 or r["retirement_successor_generations"] != [[0, 0], [2, 1], [3, 1]]: fail(f"{source}: journal/tenant/retirement versions invalid")

    bound = BOUNDS[family]
    if r["recovered_acknowledgements"] < 1 or r["max_ack_gap_bound_ms"] != bound or not 0 < r["max_ack_gap_ms"] <= bound or r["operation_bound_ms"] != 240_000 or not 0 < r["operation_elapsed_ms"] < 240_000 or not 0 < r["kill_ms"] <= r["restart_ms"] or r["publication_ms"] <= 0: fail(f"{source}: timing/count bounds invalid")
    if not all(r[key] for key in {"post_publication_r2_ack", "post_publication_r3_ack", "predecessor_topic_absent", "sentinel_topic_present"}): fail(f"{source}: terminal summary invariant false")
    if r["left_endpoint"] == r["right_endpoint"] or (r["left_wal_generation"], r["right_wal_generation"]) != (1, 1): fail(f"{source}: successor identity invalid")
    expected_topics = sorted({f"__gres_wal.{r['tenant_id']}.r0", f"__gres_wal.{r['tenant_id']}.r2.g0000000001", f"__gres_wal.{r['tenant_id']}.r3.g0000000001", r["sentinel_topic"]})
    if not r["sentinel_topic"].startswith("g8-sentinel-") or r["topology_topics"] != expected_topics: fail(f"{source}: exact topology topics differ")

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
        if name=="markers": response={"result":"markers","markers":[wire],"left_markers":[],"right_markers":[wire],"digest":marker_digest([(700,52,1)])}
        proofs.append({"sequence":sequence,"timestamp_ms":sequence+1,"operation":name,"endpoint":"127.0.0.1:9000","range_id":1,"generation":0,"operation_id":operation,"request":request,"response":response,"request_sha256":digest(compact(request)),"response_sha256":digest(compact(response)),"replay_count":1})
    result={key:0 for key in INTS}; result.update({key:"x" for key in STRINGS}); result.update({key:False for key in BOOLS}); result.update({key:[] for key in LISTS})
    result.update({"schema_version":1,"family":family,"case":case,"tenant_id":tenant,"operation_id":operation,"evidence_id":digest(f"{family}\0{case}\0{tenant}\0{operation}".encode()),"acknowledged_rows":1,"recovered_acknowledgements":1,"max_ack_gap_ms":1,"max_ack_gap_bound_ms":BOUNDS[family],"operation_elapsed_ms":2,"operation_bound_ms":240000,"marker_count":1,"right_marker_count":1,"delete_count":1,"old_pid":index*3+1,"new_pid":index*3+2,"old_source_pid":index*3+1,"new_source_pid":index*3+2,"workload_process_group":index*3+3,"kill_ms":1,"restart_ms":2,"publication_ms":3,"left_wal_generation":1,"right_wal_generation":1,"operation_revision":9,"operation_attempts":2,"tenant_record_version":6,"source_record_version":4,"retirement_successor_generations":[[0,0],[2,1],[3,1]],"sentinel_topic":sentinel,"left_endpoint":"left","right_endpoint":"right","marker_response_digest":marker_digest([(700,52,1)]),"post_publication_r2_ack":True,"post_publication_r3_ack":True,"predecessor_topic_absent":True,"sentinel_topic_present":True,"workload_process_reaped":True,"new_source_pid_alive_at_verification":True,"topology_topics":sorted([f"__gres_wal.{tenant}.r0",f"__gres_wal.{tenant}.r2.g0000000001",f"__gres_wal.{tenant}.r3.g0000000001",sentinel]),"reopened_oracle_rows":[row],"direct_physical_rows":[{"range_id":2,**row}],"sql_union_rows":[row],"source_markers":[marker],"right_markers":[marker],"authenticated_receipts":proofs,"delete_attempts":[{"targets":[f"__gres_wal.{tenant}.r1"],"outcome":"deleted"}]})
    return result

def expect_bad(record: dict[str,Any], family:str, case:str, mutate:Callable[[dict[str,Any]],None], label:str) -> None:
    changed=copy.deepcopy(record); mutate(changed)
    try: validate_record(changed,family,case,label)
    except ValidationError: return
    fail(f"negative unexpectedly passed: {label}")
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
        negatives=[
            ("evidence id",lambda r:r.__setitem__("evidence_id","0"*64)),("wrong family",lambda r:r.__setitem__("family","source_restore")),("wrong case",lambda r:r.__setitem__("case",EXPECTED[family][1])),("schema missing",lambda r:r.pop("delete_count")),("oracle mismatch",lambda r:r["reopened_oracle_rows"][0].__setitem__("seq",99)),("direct ownership",lambda r:r["direct_physical_rows"][0].__setitem__("range_id",3)),("direct duplicate",lambda r:r["direct_physical_rows"].append(copy.deepcopy(r["direct_physical_rows"][0]))),("sql mismatch",lambda r:r["sql_union_rows"].clear()),("row count",lambda r:r.__setitem__("acknowledged_rows",2)),("marker overlap",lambda r:r.__setitem__("left_markers",r["right_markers"])),("marker order",lambda r:r.__setitem__("source_markers",r["source_markers"]*2)),("marker digest",lambda r:r.__setitem__("marker_response_digest","0"*64)),("marker response",lambda r:r["authenticated_receipts"][3]["response"].__setitem__("right_markers",[])),("receipt hash",lambda r:r["authenticated_receipts"][0].__setitem__("request_sha256","0"*64)),("receipt identity",lambda r:r["authenticated_receipts"][0].__setitem__("operation_id","wrong")),("receipt operation",lambda r:r["authenticated_receipts"][0].__setitem__("operation","pause")),("receipt response",lambda r:r["authenticated_receipts"][0]["response"].__setitem__("result","rejected")),("receipt order",lambda r:r["authenticated_receipts"][0].__setitem__("sequence",99)),("receipt timestamp",lambda r:r["authenticated_receipts"][0].__setitem__("timestamp_ms",99)),("receipt replay",lambda r:r["authenticated_receipts"][0].__setitem__("replay_count",0)),("receipt missing",lambda r:r["authenticated_receipts"].pop()),("delete target",lambda r:r["delete_attempts"][0].__setitem__("targets",["other"])),("delete outcome",lambda r:r["delete_attempts"][0].__setitem__("outcome","error")),("delete count",lambda r:r.__setitem__("delete_count",2)),("unrelated delete",lambda r:r.__setitem__("unrelated_delete_attempted",True)),("pid identity",lambda r:r.__setitem__("old_source_pid",999)),("old pid alive",lambda r:r.__setitem__("old_source_pid_alive",True)),("new pid dead at verify",lambda r:r.__setitem__("new_source_pid_alive_at_verification",False)),("new pid alive terminal",lambda r:r.__setitem__("new_source_pid_alive",True)),("workload alive",lambda r:r.__setitem__("workload_process_group_alive",True)),("journal revision",lambda r:r.__setitem__("operation_revision",0)),("operation attempts",lambda r:r.__setitem__("operation_attempts",0)),("tenant version",lambda r:r.__setitem__("tenant_record_version",4)),("retirement source",lambda r:r.__setitem__("retirement_source_generation",1)),("retirement versions",lambda r:r.__setitem__("retirement_successor_generations",[[2,1],[3,2]])),("endpoint",lambda r:r.__setitem__("right_endpoint","left")),("generation",lambda r:r.__setitem__("right_wal_generation",2)),("topics",lambda r:r["topology_topics"].pop()),("sentinel",lambda r:r.__setitem__("sentinel_topic","bad")),("false invariant",lambda r:r.__setitem__("post_publication_r2_ack",False)),("wrong bound",lambda r:r.__setitem__("max_ack_gap_bound_ms",25000)),("schema extra",lambda r:r.__setitem__("extra",1))]
        for label,mutation in negatives: expect_bad(good,family,case,mutation,label)
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
