#!/usr/bin/env python3
"""End-to-end test: per-block validation report, quarantine, restore."""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.node import Node
from backend.server import create_app
from backend.storage import read_json

PASS = []
FAIL = []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(("✓" if cond else "✗"), name, ("-- " + str(extra) if extra and not cond else ""))


def make_node(data_dir, node_id="t1"):
    cfg = {
        "node_id": node_id, "port": 9999, "host": "127.0.0.1",
        "data_dir": data_dir, "peers": [], "mine": False,
        "mining_interval": 999,
        "INITIAL_DIFFICULTY_BITS": 8,   # fast mining for tests
        "TARGET_BLOCK_TIME": 10,
        "DIFFICULTY_ADJUST_INTERVAL": 5,
        "DIFFICULTY_ADJUST_MAX_FACTOR": 4,
        "DIFFICULTY_ADJUST_MIN_FACTOR": 0.25,
        "COINBASE_REWARD": 50.0,
        "MAX_TX_PER_BLOCK": 200,
        "MAX_BLOCK_FUTURE_DRIFT": 120,
        "MINING_INTERVAL": 999,
        "PEER_DIAL_TIMEOUT": 3,
        "SANDBOX_TIMEOUT": 3.0,
        "SANDBOX_MAX_PRINT": 50000,
        "CONTRACT_MAX_STATE_KEYS": 2000,
        "CONTRACT_MAX_EVENTS": 1000,
        "CONTRACT_MAX_CODE_BYTES": 65536,
    }
    node = Node(cfg)
    node.start()
    return node


def main():
    tmp = tempfile.mkdtemp(prefix="lc-quarantine-test-")
    try:
        node = make_node(tmp)
        bc = node.blockchain
        addr, _ = node.wallets.create("miner")
        addr2, _ = node.wallets.create("bob")

        # Mine 6 blocks; put a real transfer in block 3's range.
        node.mine_block(addr)                      # 1
        node.mine_block(addr)                      # 2
        tx, err = node.create_transfer(addr, addr2, 5.0, 0.1)
        assert err is None, err
        ok, reason = node.submit_transaction(tx, broadcast=False)
        assert ok, reason
        node.mine_block(addr)                      # 3 (contains transfer)
        node.mine_block(addr)                      # 4
        node.mine_block(addr)                      # 5
        node.mine_block(addr)                      # 6
        check("setup: chain height 6", bc.height == 6, bc.height)
        check("setup: txpool empty after mining", node.txpool.size() == 0)

        # ---- 1. clean detailed validation --------------------------------
        rep = bc.validate_chain_detailed()
        check("clean chain validates", rep["valid"], rep["errors"])
        check("report has per-block entries", len(rep["blocks"]) == 7)
        check("all blocks ok", all(b["status"] == "ok" for b in rep["blocks"]))

        # ---- 2. tamper block 4 file (valid JSON, modified content) -------
        bp4 = node.paths.block_path(4)
        data = read_json(bp4)
        data["transactions"][0]["amount"] = 9999.0   # tamper coinbase amount
        with open(bp4, "w") as f:
            json.dump(data, f)

        rep = bc.validate_chain_detailed()
        check("tampered chain invalid", not rep["valid"])
        check("corrupt list == [4]", rep["corrupt"] == [4], rep["corrupt"])
        b4 = next(b for b in rep["blocks"] if b["height"] == 4)
        check("block 4 status corrupt", b4["status"] == "corrupt")
        check("block 4 has specific issues", len(b4["issues"]) >= 1)
        msgs = " | ".join(i["message"] for i in b4["issues"])
        print("   block 4 issues:", msgs)
        check("other blocks still ok",
              all(b["status"] == "ok" for b in rep["blocks"]
                  if b["height"] != 4))

        # ---- 3. quarantine requires confirm (API-level check later) ------
        ok, msg, batch = bc.quarantine(4, reason="测试隔离",
                                       issues=b4["issues"])
        check("quarantine ok", ok, msg)
        check("chain rolled back to 3", bc.height == 3, bc.height)
        check("batch recorded", batch and batch["from_height"] == 4
              and batch["to_height"] == 6)
        check("batch dir exists with moved files",
              os.path.exists(os.path.join(
                  node.paths.quarantine_dir, batch["id"], "blocks",
                  "000004.json")))
        check("block files moved off main dir",
              not os.path.exists(bp4))
        check("state snapshot at 3 intact",
              os.path.exists(node.paths.state_path(3)))

        # re-admit txs like the server endpoint does
        from backend.transaction import Transaction
        for txd in batch["txs"]:
            node.txpool.re_admit([Transaction.from_dict(txd)])
        check("no txs in batch from blocks 4-6 (they were coinbase-only)",
              len(batch["txs"]) == 0, len(batch["txs"]))

        # ---- 4. validation after quarantine: gap rows --------------------
        rep = bc.validate_chain_detailed()
        check("chain valid after quarantine", rep["valid"], rep["errors"])
        qrows = [b for b in rep["blocks"] if b["status"] == "quarantined"]
        check("quarantined rows 4..6 in report",
              [b["height"] for b in qrows] == [4, 5, 6],
              [b["height"] for b in qrows])
        check("public summary shows active batch",
              len(bc.quarantine_public_summary()) == 1)

        # ---- 5. chain continues mining from correct position -------------
        status, message, h = node.mine_block(addr)
        check("mining continues at height 4", status == "extended" and h == 4,
              (status, message, h))
        check("new head links to block 3",
              bc.head.prev_hash == bc.get_block(3).hash)

        # ---- 6. restore conflicts with new blocks over the gap -----------
        ok, msg = bc.restore_quarantine(batch["id"])
        check("restore blocked while gap overwritten", not ok)
        print("   restore conflict msg:", msg)

        # roll back the replacement block, then restore works
        ok, msg = bc.rollback(3)
        check("rollback to 3 ok", ok, msg)
        ok, msg = bc.restore_quarantine(batch["id"])
        check("restore ok after rollback", ok, msg)
        check("chain back to height 6", bc.height == 6, bc.height)
        rep = bc.validate_chain_detailed()
        check("tampered block 4 flagged again", rep["corrupt"] == [4],
              rep["corrupt"])
        check("batch marked restored",
              bc.quarantine_records()[0]["restored"] is True)
        check("no active quarantine left",
              len(bc.quarantine_public_summary()) == 0)

        # ---- 7. un-parseable file + restart: tolerant load ---------------
        bp5 = node.paths.block_path(5)
        with open(bp5, "w") as f:
            f.write("{not valid json!!!")
        node2 = make_node(tmp, node_id="t1")   # reload from same data dir
        bc2 = node2.blockchain
        check("node restarts despite corrupt block 5", bc2._loaded)
        check("chain truncated to 4", bc2.height == 4, bc2.height)
        check("load_errors recorded", len(bc2.load_errors) >= 1
              and bc2.load_errors[0]["height"] == 5, bc2.load_errors)
        rep2 = bc2.validate_chain_detailed()
        check("report flags height 5 corrupt", 5 in rep2["corrupt"],
              rep2["corrupt"])
        b5 = next(b for b in rep2["blocks"] if b["height"] == 5)
        check("height 5 issue is unreadable file",
              any(i["code"] == "file_unreadable" for i in b5["issues"]),
              b5["issues"])
        check("height 6 flagged (linkage unverifiable)",
              6 in rep2["corrupt"], rep2["corrupt"])

        # ---- 8. quarantine orphan files above the truncated head ---------
        ok, msg, batch2 = bc2.quarantine(5, reason="损坏文件隔离")
        check("orphan quarantine ok", ok, msg)
        check("chain still at 4", bc2.height == 4)
        check("files 5,6 moved", not os.path.exists(bp5)
              and not os.path.exists(node2.paths.block_path(6)))
        rep2 = bc2.validate_chain_detailed()
        # block 4 is still the tampered file restored in step 6 — the report
        # must keep flagging exactly it, and nothing else.
        check("only still-tampered block 4 flagged after orphan quarantine",
              rep2["corrupt"] == [4], rep2["corrupt"])
        qrows = [b for b in rep2["blocks"] if b["status"] == "quarantined"]
        check("gap rows 5..6 shown", [b["height"] for b in qrows] == [5, 6],
              [b["height"] for b in qrows])

        # restore the orphan quarantine (no new blocks mined yet)
        ok, msg = bc2.restore_quarantine(batch2["id"])
        check("orphan restore ok", ok, msg)
        check("chain re-truncated to 4 (block5 still corrupt)",
              bc2.height == 4, bc2.height)
        check("corrupt file back on disk", os.path.exists(bp5))

        # ---- 9. HTTP API smoke test --------------------------------------
        app = create_app(node2)
        c = app.test_client()

        r = c.post("/api/admin/validate")
        d = r.get_json()
        check("API validate returns detailed report",
              r.status_code == 200 and "blocks" in d and not d["valid"])

        r = c.post("/api/admin/quarantine", json={"height": 5})
        check("API quarantine without confirm rejected",
              r.status_code == 400 and not r.get_json()["ok"])

        r = c.post("/api/admin/quarantine",
                   json={"height": 5, "confirm": True, "reason": "api测试"})
        d = r.get_json()
        check("API quarantine ok", d.get("ok"), d)
        bid = d["batch"]["id"]

        r = c.get("/api/chain")
        d = r.get_json()
        check("API /api/chain carries quarantined info",
              len(d.get("quarantined", [])) == 1, d.get("quarantined"))

        r = c.get("/api/block/5")
        d = r.get_json()
        check("API block/5 reports quarantined",
              r.status_code == 404 and d.get("quarantined") is True, d)

        r = c.get("/api/admin/quarantine")
        d = r.get_json()
        check("API quarantine list has batch",
              any(b["id"] == bid for b in d["batches"]))
        check("batch txs stripped from API",
              all("txs" not in b for b in d["batches"]))

        r = c.post("/api/admin/quarantine/restore", json={"id": bid})
        d = r.get_json()
        check("API restore ok", d.get("ok"), d)

        r = c.get("/api/chain")
        check("quarantined cleared after restore",
              r.get_json().get("quarantined") == [])

        # genesis cannot be quarantined
        r = c.post("/api/admin/quarantine",
                   json={"height": 0, "confirm": True})
        check("genesis quarantine rejected", r.status_code == 400)

    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    print(f"passed: {len(PASS)}, failed: {len(FAIL)}")
    if FAIL:
        print("FAILURES:", *FAIL, sep="\n  - ")
        sys.exit(1)


if __name__ == "__main__":
    main()
