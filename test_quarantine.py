"""End-to-end smoke test for per-block damage reporting and quarantine."""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.config import build_config
from backend.node import Node

TMP = tempfile.mkdtemp(prefix="lc-qtest-")


def make_node(node_id="node1", port=8100):
    args = type("A", (), {})()
    args.id = node_id
    args.port = port
    args.host = "127.0.0.1"
    args.peers = None
    args.data_dir = os.path.join(TMP, node_id)
    args.mine = False
    args.seed = False
    args.no_mine = True
    args.mining_interval = None
    cfg = build_config(args)
    cfg["mine"] = False
    n = Node(cfg)
    n.start()
    return n


def write_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f)


read_json_file = lambda p: json.load(open(p))

failures = []


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        failures.append(msg)


try:
    # ---- 1. mine 8 blocks ------------------------------------------------
    node = make_node()
    for _ in range(8):
        status, msg, h = node.mine_block()
        assert status == "extended", msg
    original_height = node.blockchain.height
    check(original_height == 8, f"mined 8 blocks (height={original_height})")

    # Remember block 4 hash and block 5/6 file contents for later assertions.
    b4_hash = node.blockchain.get_block(4).hash
    genesis_hash = node.blockchain.get_block(0).hash
    block5_path = node.paths.block_path(5)
    block6_path = node.paths.block_path(6)
    state7_path = node.paths.state_path(7)

    # ---- 2. damage block files while node is "offline" -------------------
    del node

    # 2a. Tamper header nonce of block 4 -> hash mismatch + PoW invalid.
    p4 = os.path.join(TMP, "node1", "blocks", "000004.json")
    d4 = read_json_file(p4)
    d4["header"]["nonce"] = d4["header"]["nonce"] + 999983
    write_json(p4, d4)

    # 2b. Truncate block 6 file -> unreadable JSON.
    with open(block6_path, "r+") as f:
        f.seek(20)
        f.truncate()

    # 2c. Tamper state snapshot of block 7 -> state root mismatch.
    d7s = read_json_file(state7_path)
    first_acct = next(iter(d7s.get("accounts", {})))
    d7s["accounts"][first_acct]["balance"] += 12345.0
    write_json(state7_path, d7s)

    # ---- 3. reboot: chain must load as valid prefix (#0..#3) -------------
    node = make_node()
    bc = node.blockchain
    check(bc.height == 3, f"chain truncated at last good block (#{bc.height})")
    check(len(bc.startup_damage) > 0,
          f"startup damage recorded ({len(bc.startup_damage)} entries)")

    # Mining must be blocked while evidence is on disk.
    status, msg, h = node.mine_block()
    check(status == "blocked_by_damage", f"mining blocked: {status}")

    # ---- 4. per-block report ---------------------------------------------
    report = bc.scan_blocks()
    by_h = {e["height"]: e for e in report["blocks"]}
    check(not report["valid"], "report says chain not fully valid")
    codes4 = {x["code"] for x in by_h[4]["errors"]}
    check(by_h[4]["status"] == "damaged", "block 4 marked damaged")
    check("HASH_MISMATCH" in codes4, f"block4 hash mismatch reported: {codes4}")
    check(by_h[5]["status"] == "orphan",
          f"block 5 is orphan (got {by_h[5]['status']})")
    check(by_h[6]["status"] == "damaged",
          f"block 6 marked damaged (unreadable json; got {by_h[6]['status']})")
    check({"FILE_UNREADABLE"} <= {x["code"] for x in by_h[6]["errors"]},
          "block6 FILE_UNREADABLE")
    check(by_h[7]["status"] == "orphan",
          f"block 7 orphan due to state-tamper (got {by_h[7]['status']})")
    codes7 = {x["code"] for x in by_h[7]["errors"]}
    check("STATE_ROOT_MISMATCH" in codes7, f"block7 state root mismatch: {codes7}")
    check(by_h[8]["status"] == "orphan" or by_h[8]["status"] == "ok",
          f"block 8 reachable in report (got {by_h[8]['status']})")

    # Quarantine must reject healthy block selection.
    plan, err = bc.quarantine_plan([5])
    check(err is not None, "cannot quarantine a non-damaged block")
    plan, err = bc.quarantine_plan([4])
    check(err is not None and "必须一并隔离" in err,
          "must include all damaged blocks above first pick")

    # Plan with both damaged blocks.
    plan, err = bc.quarantine_plan([4, 6])
    check(err is None, f"plan accepted for 4 & 6 ({err})")
    q_heights = sorted(x["height"] for x in plan["quarantined"])
    d_heights = sorted(x["height"] for x in plan["detached"])
    check(q_heights == [4, 6], f"quarantined set = {q_heights}")
    check(d_heights == [5, 7, 8], f"detached set = {d_heights}")

    # ---- 4c. undo works immediately after an isolation (no mining yet) ----
    op_early, err = node.quarantine_blocks([4, 6], note="first isolation")
    check(err is None, f"first quarantine ok ({err})")
    check(bc.height == 3, "tip at #3 after first isolation")
    _, _, err = node.restore_quarantine(op_early["id"])
    check(err is None, "undo right after quarantine succeeds")
    check(os.path.exists(p4), "corrupt #4 file restored to chain dir")
    h4_back = {e["height"]: e for e in bc.scan_blocks()["blocks"]}[4]
    check(h4_back["status"] == "damaged", "restored #4 reported damaged again")

    # ---- 5. execute quarantine (confirmation enforced at API level only) -
    op, err = node.quarantine_blocks([4, 6], note="test isolation")
    check(err is None, f"quarantine executed ({err})")
    check(not os.path.exists(p4), "block 4 file removed from chain dir")
    check(not os.path.exists(block6_path), "block 6 file removed from chain dir")
    check(os.path.isdir(os.path.join(TMP, "node1", "quarantine", "ops", op["id"])),
          "operation folder created")

    # Chain is browsable: summary contains isolated blocks after tip.
    summary = bc.chain_summary()
    summary_by_h = {b["index"]: b for b in summary}
    check(summary_by_h[4]["status"] == "quarantined",
          "summary marks #4 quarantined")
    check(summary_by_h[4]["quarantined"] is True, "flag quarantined=True")
    check(summary_by_h[5]["status"] == "detached",
          "summary marks #5 detached")
    check(summary_by_h[7]["status"] == "detached",
          "summary marks #7 detached")
    check(summary_by_h[3]["status"] == "ok", "active block #3 status ok")

    # Explorer lookup of an isolated block.
    block, status = bc.get_any_block(5)
    check(block is not None and status == "detached",
          "get_any_block finds detached #5")
    block4, status4 = bc.get_any_block(4)
    # block 4 is parseable (we changed nonce) -> still readable
    check(status4 == "quarantined", "get_any_block finds quarantined #4")

    # Damage is resolved: mining resumes and extends from #3.
    status, msg, h = node.mine_block()
    check(status == "extended", f"mining resumes after quarantine: {status} {msg}")
    check(bc.height == 4, f"new block mined at #4 (height={bc.height})")
    new4_hash = bc.get_block(4).hash
    check(new4_hash != b4_hash, "replacement block differs from corrupt one")

    # Other normal blocks (#0..#3) untouched.
    check(bc.get_block(0).hash == genesis_hash, "genesis intact")
    check(bc.get_block(3).hash is not None, "#3 still present")

    # ---- 6. undo must refuse because mining diverged the chain tip --------
    heights, residual, err = node.restore_quarantine(op["id"])
    check(err is not None and ("覆盖" in err or "分叉" in err),
          f"restore blocked after chain diverged: {err}")
    check(bc.quarantine.get_operation(op["id"])["active"],
          "operation still active after refused restore")

    # Roll back the replacement block, then restore must succeed.
    ok, msg = bc.rollback(3)
    check(ok, f"rollback to #3: {msg}")
    # rollback deletes block/state files above 3 but only active dir; fine.
    heights, residual, err = node.restore_quarantine(op["id"])
    check(err is None, f"restore succeeds after rollback ({err})")
    check(bc.height == 3 and len(residual) > 0,
          f"restored corrupt files -> chain stays on valid prefix #3 "
          f"(height={bc.height}, residual={[e['height'] for e in residual]})")
    check(os.path.exists(p4), "block 4 file restored to chain dir")
    check(os.path.exists(block6_path), "block 6 file restored to chain dir")

    # Restored damage is reported again (nothing hidden) and can be isolated
    # a second time.
    report2 = bc.scan_blocks()
    by_h2 = {e["height"]: e for e in report2["blocks"]}
    check(by_h2[4]["status"] == "damaged",
          "restored corrupt #4 reported as damaged again")
    op2, err = node.quarantine_blocks([4, 6], note="re-isolate")
    check(err is None, f"re-quarantine works ({err})")
    check(bc.height == 3, "chain back at #3 after re-isolation")
    check(len(bc.quarantine_operations()) == 3,
          "three operations recorded (early undo, main isolation, final "
          "re-isolation)")
    active_ops = [o for o in bc.quarantine_operations() if o["active"]]
    check(len(active_ops) == 1, f"exactly one active operation ({len(active_ops)})")

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S)")
        sys.exit(1)
    print("ALL CHECKS PASSED")
finally:
    shutil.rmtree(TMP, ignore_errors=True)


# -------------------------------------------------------------------------- #
# Second scenario: tampering only a state snapshot must not take the healthy
# block offline — the snapshot is regenerated by replay on boot.
# -------------------------------------------------------------------------- #
def run_second_scenario():
    print("\n== scenario 2: state-only tamper is auto-repaired on boot ==")
    tmp = tempfile.mkdtemp(prefix="lc-qtest2-")
    args2 = type("A", (), {})()
    args2.id = "s2"; args2.port = 8500; args2.host = "127.0.0.1"
    args2.peers = None; args2.data_dir = os.path.join(tmp, "s2")
    args2.mine = False; args2.seed = False; args2.no_mine = True
    args2.mining_interval = None
    node = Node(build_config(args2))
    node.start()
    for _ in range(5):
        st, _, _ = node.mine_block()
        assert st == "extended"
    height_before = node.blockchain.height
    assert height_before == 5

    # Tamper ONLY the state snapshot of block 2; block file stays intact.
    sp = node.paths.state_path(2)
    sd = json.load(open(sp))
    key = next(iter(sd.get("accounts", {})))
    sd["accounts"][key]["balance"] += 999.0
    write_json(sp, sd)
    # Also delete snapshot of block 4: missing snapshot must be rebuilt too.
    os.remove(node.paths.state_path(4))
    del node

    node = Node(build_config(args2))
    node.start()
    bc = node.blockchain
    check(bc.height == height_before,
          f"state-only damage keeps full chain (height={bc.height}, want 5)")
    check(not bc.startup_damage, "no startup damage for state-only corruption")
    snap2 = json.load(open(bc.paths.state_path(2)))
    check(abs(float(snap2["accounts"][key]["balance"]) -
              (float(sd["accounts"][key]["balance"]) - 999.0)) < 1e-6,
          "tampered snapshot #2 rewritten with recomputed balance")
    check(os.path.exists(bc.paths.state_path(4)),
          "missing snapshot #4 regenerated on boot")
    report = bc.scan_blocks()
    codes2 = {x["code"] for x in
              next(e for e in report["blocks"] if e["height"] == 2)["errors"]}
    check(not codes2, f"snapshot #2 verifies after repair ({codes2})")
    check(report["valid"], "full report is valid after auto-repair")
    # Mining continues normally.
    st, _, _ = node.mine_block()
    check(st == "extended", f"mining normal after state repair: {st}")
    shutil.rmtree(tmp, ignore_errors=True)


# -------------------------------------------------------------------------- #
# Third scenario: quarantine state must survive a node restart.
# -------------------------------------------------------------------------- #
def run_third_scenario():
    print("\n== scenario 3: quarantine persists across restart ==")
    tmp = tempfile.mkdtemp(prefix="lc-qtest3-")
    args3 = type("A", (), {})()
    args3.id = "s3"; args3.port = 8700; args3.host = "127.0.0.1"
    args3.peers = None; args3.data_dir = os.path.join(tmp, "s3")
    args3.mine = False; args3.seed = False; args3.no_mine = True
    args3.mining_interval = None
    node = Node(build_config(args3))
    node.start()
    for _ in range(4):
        st, _, _ = node.mine_block()
        assert st == "extended"
    p = node.paths.block_path(2)
    d = json.load(open(p)); d["header"]["nonce"] += 3; json.dump(d, open(p, "w"))
    del node

    node = Node(build_config(args3)); node.start()
    check(node.blockchain.height == 1, "boot stopped at good prefix #1")
    op, err = node.quarantine_blocks([2], note="persist test")
    check(err is None, f"quarantine before restart ({err})")
    st, _, _ = node.mine_block()
    check(st == "extended", "mining resumes once")
    del node

    # Restart: the active operation and detached/quarantined records survive.
    node = Node(build_config(args3)); node.start()
    bc = node.blockchain
    check(bc.height == 2, f"replacement #2 present after restart ({bc.height})")
    active = bc.quarantine.active_blocks()
    check(sorted(active) == [2, 3, 4], f"quarantine records survive: {sorted(active)}")
    check(active[2]["status"] == "quarantined", "old #2 still quarantined")
    check(active[3]["status"] == "detached", "#3 still detached")
    summary = {b["index"]: b for b in bc.chain_summary()}
    check(summary[2]["replaced"] is True, "replaced flag correct after restart")
    check(summary[3]["status"] == "detached", "#3 visible as detached in explorer")
    check(not bc.unprocessed_damage(), "no unprocessed damage after restart")
    st, _, _ = node.mine_block()
    check(st == "extended", f"mining continues normally after restart: {st}")
    shutil.rmtree(tmp, ignore_errors=True)


run_second_scenario()
run_third_scenario()
