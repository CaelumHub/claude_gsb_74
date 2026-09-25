#!/usr/bin/env python3
"""Test: quarantined blocks' transactions return to the pool and re-mine."""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.node import Node
from backend.server import create_app
from backend.storage import read_json

FAIL = []


def check(name, cond, extra=""):
    print(("✓" if cond else "✗"), name, ("-- " + str(extra) if extra and not cond else ""))
    if not cond:
        FAIL.append(name)


def make_node(data_dir, node_id="t1"):
    cfg = {
        "node_id": node_id, "port": 9998, "host": "127.0.0.1",
        "data_dir": data_dir, "peers": [], "mine": False,
        "mining_interval": 999, "INITIAL_DIFFICULTY_BITS": 8,
        "TARGET_BLOCK_TIME": 10, "DIFFICULTY_ADJUST_INTERVAL": 5,
        "DIFFICULTY_ADJUST_MAX_FACTOR": 4, "DIFFICULTY_ADJUST_MIN_FACTOR": 0.25,
        "COINBASE_REWARD": 50.0, "MAX_TX_PER_BLOCK": 200,
        "MAX_BLOCK_FUTURE_DRIFT": 120, "MINING_INTERVAL": 999,
        "PEER_DIAL_TIMEOUT": 3, "SANDBOX_TIMEOUT": 3.0,
        "SANDBOX_MAX_PRINT": 50000, "CONTRACT_MAX_STATE_KEYS": 2000,
        "CONTRACT_MAX_EVENTS": 1000, "CONTRACT_MAX_CODE_BYTES": 65536,
    }
    node = Node(cfg)
    node.start()
    return node


def main():
    tmp = tempfile.mkdtemp(prefix="lc-quarantine-tx-")
    try:
        node = make_node(tmp)
        bc = node.blockchain
        app = create_app(node)
        c = app.test_client()
        addr, _ = node.wallets.create("miner")
        bob, _ = node.wallets.create("bob")

        node.mine_block(addr)   # 1
        node.mine_block(addr)   # 2
        # transfer goes into block 3
        tx, err = node.create_transfer(addr, bob, 7.0, 0.1)
        assert err is None
        ok, reason = node.submit_transaction(tx, broadcast=False)
        assert ok, reason
        node.mine_block(addr)   # 3 contains the transfer
        node.mine_block(addr)   # 4
        bob_balance_before = bc.state.balance(bob)
        txid = tx.txid
        check("transfer mined in block 3",
              any(t.txid == txid for t in bc.get_block(3).transactions))
        check("bob credited", bob_balance_before == 7.0, bob_balance_before)
        check("pool empty", node.txpool.size() == 0)

        # tamper block 3 on disk, then quarantine from height 3 via the API
        bp3 = node.paths.block_path(3)
        data = read_json(bp3)
        data["header"]["nonce"] = 123456789   # break PoW / hash
        with open(bp3, "w") as f:
            json.dump(data, f)
        rep = c.post("/api/admin/validate").get_json()
        # Block 3 is the root cause (PoW broken + disk/memory mismatch);
        # block 4 is flagged as a cascade because its prev_hash no longer
        # matches the tampered block 3 — exactly what a per-block report
        # should distinguish.
        check("block 3 flagged as root cause, 4 as cascade",
              rep["corrupt"] == [3, 4], rep["corrupt"])
        issues3 = next(b for b in rep["blocks"] if b["height"] == 3)["issues"]
        issues4 = next(b for b in rep["blocks"] if b["height"] == 4)["issues"]
        print("   block 3 issues:",
              " | ".join(i["message"] for i in issues3))
        print("   block 4 issues:",
              " | ".join(i["message"] for i in issues4))
        check("block 3 root-cause issues",
              any(i["code"] == "structure" for i in issues3)
              and any(i["code"] == "disk_mismatch" for i in issues3),
              issues3)
        check("block 4 cascade issue is linkage",
              [i["code"] for i in issues4] == ["linkage"], issues4)

        r = c.post("/api/admin/quarantine",
                   json={"height": 3, "confirm": True,
                         "reason": "PoW 损坏"}).get_json()
        check("quarantine ok", r.get("ok"), r)
        check("height back to 2", bc.height == 2, bc.height)
        check("bob balance rolled back", bc.state.balance(bob) == 0.0,
              bc.state.balance(bob))
        check("transfer re-admitted to pool",
              node.txpool.contains(txid), node.txpool.txids())

        # mine again: the re-admitted transfer is re-packaged at height 3
        status, message, h = node.mine_block(addr)
        check("re-mined at height 3", status == "extended" and h == 3,
              (status, message, h))
        check("transfer re-mined in new block 3",
              any(t.txid == txid for t in bc.get_block(3).transactions))
        check("pool empty again", node.txpool.size() == 0)
        check("bob re-credited", bc.state.balance(bob) == 7.0,
              bc.state.balance(bob))
        rep = bc.validate_chain_detailed()
        check("chain valid after re-mine", rep["valid"], rep["errors"])

        # restore must fail: height 3 was re-mined over the gap
        batch_id = r["batch"]["id"]
        rr = c.post("/api/admin/quarantine/restore",
                    json={"id": batch_id}).get_json()
        check("restore rejected after re-mining", not rr.get("ok"), rr)

        # roll back the replacement block and restore: tampered block returns
        ok, msg = bc.rollback(2)
        assert ok, msg
        rr = c.post("/api/admin/quarantine/restore",
                    json={"id": batch_id}).get_json()
        check("restore ok after rollback", rr.get("ok"), rr)
        # The tolerant reload truncates where the tampered block 3 breaks the
        # linkage of block 4 — the accurate reflection of what is on disk.
        check("chain re-truncated at tampered block 3", bc.height == 3,
              bc.height)
        rep = bc.validate_chain_detailed()
        check("tampered block 3 flagged again (with cascade)",
              rep["corrupt"] == [3, 4], rep["corrupt"])
        # restored transfer tx should not linger in the pool
        check("pool clean after restore", not node.txpool.contains(txid))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if FAIL:
        print("FAILURES:", *FAIL, sep="\n  - ")
        sys.exit(1)
    print("all tx-pool quarantine tests passed")


if __name__ == "__main__":
    main()
