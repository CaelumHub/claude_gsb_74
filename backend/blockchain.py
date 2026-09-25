"""The blockchain: block application, fork resolution, reorg, and persistence.

This is the consensus core.  It owns the main chain (an in-memory list of
:class:`~block.Block`), the current :class:`~state.WorldState`, and a side-branch
store used for fork handling.  Consensus follows the *heaviest chain* rule:
when two competing branches exist, the one with greater cumulative work wins,
and the node re-organises onto it (rolling back blocks below the fork point and
re-applying the winning branch).

Persistence is per-block: ``blocks/NNNNNN.json`` holds a block and
``state/NNNNNN.json`` holds the world state *after* that height, both written
atomically and mirrored by a version ledger for rollback.
"""

import os
import shutil
import time
import uuid

from . import crypto, pow as pow_mod
from .block import Block, make_genesis_block
from .config import COINBASE_REWARD, GENESIS_PREV_HASH
from .contract import ContractEngine
from .state import WorldState, ZERO_ADDRESS
from .storage import (DataPaths, VersionLedger, atomic_write_json, read_json)
from .transaction import Transaction, TX_COINBASE


class ChainValidationError(Exception):
    pass


class _DiskChainView:
    """Minimal chain-like view over a list of blocks read from disk.

    Used by the per-block validation report to evaluate the difficulty
    schedule for blocks that may not be part of the loaded main chain.
    """

    def __init__(self, blocks, genesis_difficulty):
        self._blocks = blocks           # contiguous prefix, index == height
        self.genesis_difficulty = genesis_difficulty

    def get_block(self, height):
        if 0 <= height < len(self._blocks):
            return self._blocks[height]
        return None


class Blockchain:
    def __init__(self, cfg, paths: DataPaths):
        self.cfg = cfg
        self.paths = paths
        self.chain = []                 # main-chain blocks, index 0..height
        self.state = WorldState()
        self.fork_store = {}            # height -> list of competing blocks
        self.genesis_difficulty = float(cfg.get("INITIAL_DIFFICULTY_BITS", 16))
        self.chainwork = 0              # cumulative work of the main chain
        self.engine = ContractEngine(cfg)
        self.versions = VersionLedger(paths.versions_path)
        self.last_abandoned = []
        self.last_receipts = []
        self.load_errors = []           # problems tolerated during load()
        self._loaded = False

    # ==================================================================== #
    # Basic accessors
    # ==================================================================== #
    @property
    def height(self):
        return len(self.chain) - 1 if self.chain else -1

    @property
    def head(self):
        return self.chain[-1] if self.chain else None

    def get_block(self, height):
        if 0 <= height < len(self.chain):
            return self.chain[height]
        return None

    def get_block_by_hash(self, hash_hex):
        for b in self.chain:
            if b.hash == hash_hex:
                return b
        for blocks in self.fork_store.values():
            for b in blocks:
                if b.hash == hash_hex:
                    return b
        return None

    def has_block(self, hash_hex):
        return self.get_block_by_hash(hash_hex) is not None

    def cumulative_work_of(self, blocks):
        return sum(int(2 ** b.difficulty) for b in blocks)

    # ==================================================================== #
    # Bootstrap / persistence
    # ==================================================================== #
    def create_genesis(self, state_root=None):
        genesis = make_genesis_block(self.genesis_difficulty, state_root)
        self.chain = [genesis]
        self.state = WorldState()
        genesis.set_state_root(self.state.root())
        genesis.recompute_hash()
        self.chainwork = int(2 ** genesis.difficulty)
        self._persist_block(genesis, self.state)
        self._write_meta()
        self.versions.record(0, genesis.hash)
        self._loaded = True
        return genesis

    def load(self):
        """Load the chain from disk (or create genesis if absent).

        Tolerant of individual corrupted block files: instead of aborting the
        whole node, the main chain is truncated just before the first broken
        height and the problem is recorded in ``self.load_errors`` so the
        operator can locate, quarantine, or restore the affected blocks.
        """
        meta = read_json(self.paths.meta_path)
        if not meta or not os.path.exists(self.paths.block_path(0)):
            self.create_genesis()
            return

        height = int(meta.get("height", 0))
        self.genesis_difficulty = float(meta.get("genesis_difficulty",
                                                 self.genesis_difficulty))
        blocks = []
        self.load_errors = []
        for h in range(0, height + 1):
            data = read_json(self.paths.block_path(h))
            if data is None:
                self.load_errors.append({
                    "height": h,
                    "reason": "区块文件缺失或无法解析（JSON 损坏）",
                })
                break
            try:
                blk = Block.from_dict(data)
            except Exception as e:  # noqa: BLE001 - corrupt file structure
                self.load_errors.append({
                    "height": h,
                    "reason": f"区块文件结构损坏：{e}",
                })
                break
            blk.recompute_hash()
            if blocks and blk.prev_hash != blocks[-1].hash:
                self.load_errors.append({
                    "height": h,
                    "reason": "与前序区块的哈希链接断裂",
                })
                break
            blocks.append(blk)
        if not blocks:
            # The genesis block itself is unreadable — nothing safe to
            # truncate to; this needs manual intervention (reset or resync).
            raise ChainValidationError("genesis block missing or unreadable")
        if self.load_errors:
            broken = self.load_errors[0]
            self.load_errors.append({
                "height": None,
                "reason": f"主链已截断到高度 {blocks[-1].index}；高度 "
                          f"{broken['height']} 及之后的区块未加载，可通过"
                          f"管理后台的逐块校验定位并隔离",
            })
        self.chain = blocks
        self.state, _ = self._load_state_snapshot(blocks[-1].index)
        self.chainwork = self.cumulative_work_of(blocks)
        self._loaded = True

    def _load_state_snapshot(self, height):
        """Return ``(state, actual_height)`` from the newest readable snapshot
        at or below ``height`` (falls back to an empty state)."""
        for h in range(height, -1, -1):
            data = read_json(self.paths.state_path(h))
            if data is None:
                continue
            try:
                return WorldState.from_dict(data), h
            except Exception:  # noqa: BLE001 - try an older snapshot
                continue
        return WorldState(), -1

    def _write_meta(self):
        meta = {
            "height": self.height,
            "head_hash": self.head.hash if self.head else None,
            "genesis_difficulty": self.genesis_difficulty,
            "chainwork": self.chainwork,
            "node_id": self.cfg.get("node_id", "node"),
            "updated_at": time.time(),
        }
        atomic_write_json(self.paths.meta_path, meta)

    def _persist_block(self, block, state):
        atomic_write_json(self.paths.block_path(block.index), block.to_dict())
        atomic_write_json(self.paths.state_path(block.index), state.to_dict())

    def _delete_block_files_above(self, height):
        for f in os.listdir(self.paths.blocks_dir):
            try:
                h = int(f.split(".")[0])
            except ValueError:
                continue
            if h > height:
                os.remove(os.path.join(self.paths.blocks_dir, f))
        for f in os.listdir(self.paths.state_dir):
            try:
                h = int(f.split(".")[0])
            except ValueError:
                continue
            if h > height:
                os.remove(os.path.join(self.paths.state_dir, f))

    # ==================================================================== #
    # Transaction execution
    # ==================================================================== #
    def _execute_transaction(self, tx, state, miner, height):
        """Apply a single non-coinbase transaction; return ``(ok, receipt)``.

        The state is snapshotted first so a reverting contract call undoes its
        own effects.  Fee and nonce are applied regardless of outcome (the
        attempt still consumed resources).
        """
        before = state.copy()
        receipt = {"txid": tx.txid, "ok": True, "error": None, "events": [],
                   "return": None, "transfers": [], "type": tx.tx_type,
                   "contract": None}
        try:
            if tx.tx_type == "transfer":
                if state.balance(tx.sender) < tx.amount + tx.fee:
                    raise ChainValidationError("insufficient balance")
                state.add_balance(tx.sender, -tx.amount)
                state.add_balance(tx.to, tx.amount)
            elif tx.tx_type == "deploy":
                if state.balance(tx.sender) < tx.fee:
                    raise ChainValidationError("insufficient balance for deploy")
                address = self._contract_address(tx)
                result = self.engine.deploy(
                    tx.data.get("code", ""), tx.sender, address, state,
                    constructor=tx.data.get("constructor"), height=height)
                if not result["ok"]:
                    raise ChainValidationError(result["error"] or "deploy failed")
                receipt["events"] = result["events"]
                receipt["contract"] = address
            elif tx.tx_type == "call":
                if state.balance(tx.sender) < tx.amount + tx.fee:
                    raise ChainValidationError("insufficient balance for call")
                # Credit the contract with the attached value before invoking,
                # so msg.value / this_balance / transfer see it.
                state.add_balance(tx.sender, -tx.amount)
                state.add_balance(tx.to, tx.amount)
                result = self.engine.invoke(
                    tx.to, tx.data.get("function"), tx.data.get("args", []),
                    tx.sender, tx.amount, state, height)
                if not result["ok"]:
                    raise ChainValidationError(result["error"] or "call failed")
                receipt["events"] = result["events"]
                receipt["return"] = result["return"]
                receipt["contract"] = tx.to
            else:
                raise ChainValidationError(f"unknown type {tx.tx_type}")
        except Exception as e:  # noqa: BLE001 - revert semantics
            state = before
            receipt["ok"] = False
            receipt["error"] = str(e)

        # Fee + nonce bookkeeping is applied even when the call reverted.
        if state.balance(tx.sender) >= tx.fee:
            state.add_balance(tx.sender, -tx.fee)
            state.add_balance(miner, tx.fee)
        state.increment_nonce(tx.sender)
        return state, receipt

    def _contract_address(self, tx):
        """Derive a deterministic contract address from the deploy tx."""
        return "0xc" + crypto.sha256(tx.txid.encode()).hex()[:40]

    def apply_block(self, block, state):
        """Apply all transactions of ``block`` to ``state``; return receipts."""
        miner = ZERO_ADDRESS
        receipts = []
        for tx in block.transactions:
            if tx.is_coinbase():
                miner = tx.to
                state.add_balance(tx.to, tx.amount)
                receipts.append({"txid": tx.txid, "ok": True, "coinbase": True})
                continue
            state, receipt = self._execute_transaction(
                tx, state, miner or ZERO_ADDRESS, block.index)
            receipts.append(receipt)
        return state, receipts

    # ==================================================================== #
    # Block validation
    # ==================================================================== #
    def validate_block(self, block, prev_block):
        """Full validation of ``block`` against ``prev_block``."""
        if block.index != prev_block.index + 1:
            return False, "block index is not prev+1"
        if block.prev_hash != prev_block.hash:
            return False, "prev_hash does not match parent"
        ok, reason = block.validate_structure()
        if not ok:
            return False, reason
        if block.difficulty != pow_mod.next_difficulty(self, block):
            return False, "difficulty does not match schedule"
        # Verify transaction signatures and sender authenticity.
        for tx in block.transactions:
            if tx.is_coinbase():
                if tx.amount != COINBASE_REWARD:
                    return False, "coinbase reward mismatch"
                continue
            if not tx.validate_signature():
                return False, f"invalid signature on tx {tx.txid}"
            if tx.derived_sender() != tx.sender:
                return False, f"sender mismatch on tx {tx.txid}"
        return True, "ok"

    # ==================================================================== #
    # Consensus: adding blocks, fork handling, reorg
    # ==================================================================== #
    def add_block(self, block):
        """Add a validated block, resolving any fork it creates.

        Returns ``(status, message)`` where status is one of
        ``extended``, ``duplicate``, ``stored_fork``, ``reorg``, ``invalid``.
        """
        # Duplicate protection.
        if self.has_block(block.hash):
            return "duplicate", "block already known"

        if block.index == self.height + 1:
            prev = self.get_block(block.index - 1)
            if prev is None or block.prev_hash != prev.hash:
                # Might extend a stored fork.
                return self._maybe_extend_fork(block)
            ok, reason = self.validate_block(block, prev)
            if not ok:
                return "invalid", reason
            return self._extend(block)

        if block.index <= self.height:
            return self._maybe_extend_fork(block)

        # block.index > height + 1: we are missing ancestors -> request sync.
        return "missing", "missing ancestor blocks (need sync)"

    def _apply_and_verify(self, block, state):
        """Apply ``block`` to ``state``, verifying the committed state root."""
        new_state, receipts = self.apply_block(block, state)
        if new_state.root() != block.header.state_root:
            raise ChainValidationError(
                "state root mismatch after applying block (non-deterministic "
                "or malformed block)")
        return new_state, receipts

    def _extend(self, block):
        try:
            new_state, receipts = self._apply_and_verify(block, self.state.copy())
        except ChainValidationError as e:
            return "invalid", str(e)
        self.chain.append(block)
        self.state = new_state
        self.chainwork += int(2 ** block.difficulty)
        self.last_receipts = receipts
        self._persist_block(block, new_state)
        self._write_meta()
        self.versions.record(block.index, block.hash)
        return "extended", "chain extended"

    def _maybe_extend_fork(self, block):
        """Store a competing block, and reorg if it makes a heavier branch."""
        if block.index == self.height and block.prev_hash != self.head.prev_hash:
            # A sibling of our head (or an ancestor fork we still track).
            pass
        self.fork_store.setdefault(block.index, []).append(block)

        # Try to trace a branch from this block back to the main chain.
        branch = self._trace_branch(block)
        if branch is None:
            return "stored_fork", "competing block stored (branch incomplete)"
        branch_work = self.cumulative_work_of(branch)
        main_work_at_ancestor = self.cumulative_work_of(
            self.chain[:branch[0].index + 1]) if branch else 0
        if branch_work + main_work_at_ancestor > self.chainwork:
            return self._reorg(branch)
        return "stored_fork", "competing block stored (weaker branch)"

    def _trace_branch(self, tip):
        """Walk fork_store back to a block whose parent is on the main chain."""
        by_hash = {b.hash: b for blocks in self.fork_store.values()
                   for b in blocks}
        branch = []
        current = tip
        seen = set()
        while current is not None and current.hash not in seen:
            seen.add(current.hash)
            branch.append(current)
            if current.index == 0:
                break
            # Parent on the main chain?
            main_parent = self.get_block(current.index - 1)
            if main_parent is not None and main_parent.hash == current.prev_hash:
                branch.reverse()
                return branch
            current = by_hash.get(current.prev_hash)
        return None

    def _reorg(self, branch):
        """Switch the main chain onto a heavier fork ``branch``.

        ``branch`` is a list of blocks whose first element's parent is on the
        current main chain.  Blocks after the common ancestor are rolled back
        (their state is discarded and transactions re-admitted by the caller)
        and the branch is applied in order.
        """
        ancestor_height = branch[0].index - 1
        abandoned = self.chain[ancestor_height + 1:]

        # Roll back state to the ancestor snapshot.
        ancestor_state_data = read_json(self.paths.state_path(ancestor_height))
        state = WorldState.from_dict(ancestor_state_data)
        self.chain = self.chain[:ancestor_height + 1]
        self.chainwork = self.cumulative_work_of(self.chain)
        self.last_abandoned = list(abandoned)  # for tx re-admission by the node

        applied = 0
        all_receipts = []
        for blk in branch:
            state, receipts = self.apply_block(blk, state)
            all_receipts.extend(receipts)
            if state.root() != blk.header.state_root:
                # The winning branch must itself be consistent; if not, keep
                # the current chain and flag the reorg as failed.
                return "invalid", "fork branch failed state-root verification"
            self.chain.append(blk)
            self.chainwork += int(2 ** blk.difficulty)
            self._persist_block(blk, state)
            applied += 1

        self.state = state
        self.last_receipts = all_receipts
        self._write_meta()
        self.versions.record(self.height, self.head.hash)
        # Clean fork_store of now-main blocks.
        for b in branch:
            self.fork_store.pop(b.index, None)
        return "reorg", (f"reorg at height {ancestor_height}: "
                         f"rolled back {len(abandoned)} block(s), "
                         f"applied {applied} block(s)")

    def rollback(self, target_height):
        """Roll the chain back to ``target_height`` (admin operation)."""
        if target_height < 0 or target_height >= self.height:
            return False, "invalid target height"
        if not os.path.exists(self.paths.state_path(target_height)):
            return False, "no state snapshot at target height"
        state_data = read_json(self.paths.state_path(target_height))
        self.state = WorldState.from_dict(state_data)
        self.chain = self.chain[:target_height + 1]
        self.chainwork = self.cumulative_work_of(self.chain)
        self._delete_block_files_above(target_height)
        self._write_meta()
        self.versions.record(target_height, self.head.hash)
        # Load-time problems above the rollback point are now resolved.
        self.load_errors = [
            e for e in self.load_errors
            if e.get("height") is not None and e["height"] <= target_height
        ]
        return True, f"rolled back to height {target_height}"

    # ==================================================================== #
    # Dashboard / display aggregation helpers
    # ==================================================================== #
    def tx_type_counts(self):
        counts = {}
        for blk in self.chain:
            for tx in blk.transactions:
                cat = tx.stats_category()
                counts[cat] = counts.get(cat, 0) + 1
        return counts

    def avg_block_interval(self, window=20):
        blocks = self.chain
        if len(blocks) < 2:
            return 0.0
        start = max(1, len(blocks) - window)
        intervals = [blocks[i].elapsed_since(blocks[i - 1])
                     for i in range(start, len(blocks))]
        return sum(intervals) / len(intervals) if intervals else 0.0

    def difficulty_series(self):
        return pow_mod.difficulty_series(self)

    def top_accounts(self, limit=10):
        return self.state.top_accounts(limit=limit)

    def balance_for_display(self, address):
        return self.state.display_balance(address)

    def chain_summary(self):
        return [
            {
                "index": b.index, "hash": b.hash, "prev_hash": b.prev_hash,
                "timestamp": b.timestamp, "difficulty": b.difficulty,
                "tx_count": b.display_tx_count(), "nonce": b.nonce,
            }
            for b in self.chain
        ]

    def block_summary(self, block):
        return {
            "index": block.index, "hash": block.hash,
            "prev_hash": block.prev_hash, "timestamp": block.timestamp,
            "difficulty": block.difficulty,
            "tx_count": block.display_tx_count(), "nonce": block.nonce,
            "merkle_root": block.header.merkle_root,
            "state_root": block.state_root,
        }

    def transactions_for(self, address, limit=200):
        txs = []
        for blk in self.chain:
            for tx in blk.transactions:
                if tx.involves(address):
                    txs.append({
                        "txid": tx.txid, "type": tx.tx_type, "from": tx.sender,
                        "to": tx.to, "amount": tx.amount, "fee": tx.fee,
                        "nonce": tx.nonce, "height": blk.index,
                        "timestamp": tx.timestamp,
                    })
        txs.reverse()
        return txs[:limit]

    # ==================================================================== #
    # Tamper detection: per-block validation report
    # ==================================================================== #
    # Chinese descriptions for the structural failure reasons produced by
    # Block.validate_structure(), so the per-block report can pinpoint what
    # exactly is wrong with each damaged block.
    _REASON_ZH = {
        "missing block hash": "缺少区块哈希",
        "block hash does not match header contents":
            "区块哈希与头部内容不符（文件内容可能被篡改）",
        "proof-of-work does not satisfy difficulty": "工作量证明不满足难度要求",
        "merkle root mismatch": "Merkle 根与交易列表不符（交易可能被篡改）",
        "multiple coinbase transactions": "包含多个 coinbase 交易",
        "coinbase must be the first transaction": "coinbase 交易必须是第一笔交易",
    }

    def _disk_block_heights(self):
        """Sorted heights of all block files present on disk."""
        heights = []
        if os.path.isdir(self.paths.blocks_dir):
            for f in os.listdir(self.paths.blocks_dir):
                name, _, ext = f.partition(".")
                if ext == "json" and name.isdigit():
                    heights.append(int(name))
        return sorted(heights)

    def _quarantine_gap(self):
        """Height range of the active quarantine gap, or ``None``.

        A gap exists while an un-restored quarantine batch starts above the
        current chain head (the isolated files were moved away and no new
        blocks have been mined over the gap yet).
        """
        for b in self.quarantine_records():
            if not b.get("restored") and b["from_height"] > self.height:
                return b
        return None

    def validate_chain_detailed(self):
        """Verify every block file individually and build a per-block report.

        Each entry describes one height with ``status`` (``ok`` / ``corrupt``
        / ``quarantined``) and a list of human-readable ``issues`` explaining
        exactly what is wrong, so a single damaged block file can be located
        precisely instead of failing the whole chain check.
        """
        disk_heights = self._disk_block_heights()
        gap = self._quarantine_gap()
        max_h = max([self.height, gap["to_height"] if gap else -1]
                    + disk_heights + [-1])

        report = {
            "valid": True,
            "checked": 0,
            "height": self.height,
            "disk_blocks": len(disk_heights),
            "corrupt": [],
            "blocks": [],
            "quarantined": self.quarantine_records(public=True),
            "load_errors": list(self.load_errors),
            "errors": [],
        }

        prev_block = None         # last readable block (for linkage checks)
        prev_broken = False       # previous height could not be verified
        view_blocks = []          # contiguous readable prefix, for difficulty
        view_valid = True         # False once the prefix has a gap
        for h in range(0, max_h + 1):
            entry = {"height": h, "hash": None, "status": "ok",
                     "on_chain": h <= self.height, "issues": []}
            report["checked"] += 1

            # An active quarantine gap: files were moved aside on purpose.
            if gap and gap["from_height"] <= h <= gap["to_height"]:
                entry["status"] = "quarantined"
                entry["on_chain"] = False
                entry["issues"].append({
                    "code": "quarantined",
                    "message": f"该区块已被隔离（批次 {gap['id']}），"
                               f"可在管理后台撤销恢复",
                })
                report["blocks"].append(entry)
                prev_block = None
                prev_broken = True
                view_valid = False
                continue

            raw = read_json(self.paths.block_path(h))
            if raw is None:
                entry["status"] = "corrupt"
                entry["issues"].append({
                    "code": "file_unreadable",
                    "message": "区块文件缺失或 JSON 无法解析",
                })
                report["blocks"].append(entry)
                prev_block = None
                prev_broken = True
                view_valid = False
                continue

            try:
                blk = Block.from_dict(raw)
            except Exception as e:  # noqa: BLE001 - corrupt structure
                entry["status"] = "corrupt"
                entry["issues"].append({
                    "code": "parse_error",
                    "message": f"区块文件结构损坏：{e}",
                })
                report["blocks"].append(entry)
                prev_block = None
                prev_broken = True
                view_valid = False
                continue

            entry["hash"] = blk.hash

            # 1. Linkage with the previous readable block.
            if h > 0:
                if prev_block is not None and blk.prev_hash != prev_block.hash:
                    entry["issues"].append({
                        "code": "linkage",
                        "message": "prev_hash 与前序区块不符（哈希链接断裂）",
                    })
                elif prev_broken:
                    entry["issues"].append({
                        "code": "linkage_unknown",
                        "message": "前序区块损坏，哈希链接无法验证",
                    })

            # 2. Structure (hash recompute, PoW, merkle root, coinbase rules).
            #    Genesis is exempt from PoW, so only its hash is re-checked.
            if h == 0:
                stored = blk.hash
                if blk.recompute_hash() != stored:
                    entry["issues"].append({
                        "code": "hash_mismatch",
                        "message": self._REASON_ZH[
                            "block hash does not match header contents"],
                    })
            else:
                ok, reason = blk.validate_structure()
                if not ok:
                    entry["issues"].append({
                        "code": "structure",
                        "message": self._REASON_ZH.get(reason, reason),
                    })
                # 3. Difficulty schedule (needs the contiguous prefix view).
                if view_valid and prev_block is not None:
                    view = _DiskChainView(view_blocks, self.genesis_difficulty)
                    expected = pow_mod.next_difficulty(view, blk)
                    if blk.difficulty != expected:
                        entry["issues"].append({
                            "code": "difficulty",
                            "message": f"难度 {blk.difficulty} 与计划值 "
                                       f"{expected} 不符",
                        })
                # 4. Transaction signatures / sender authenticity.
                for tx in blk.transactions:
                    if tx.is_coinbase():
                        if tx.amount != COINBASE_REWARD:
                            entry["issues"].append({
                                "code": "coinbase_reward",
                                "message": "coinbase 奖励金额不符",
                            })
                        continue
                    if not tx.validate_signature():
                        entry["issues"].append({
                            "code": "signature",
                            "message": f"交易 {tx.txid[:16]}… 签名无效",
                        })
                    elif tx.derived_sender() != tx.sender:
                        entry["issues"].append({
                            "code": "sender",
                            "message": f"交易 {tx.txid[:16]}… 发送方与公钥不符",
                        })

            # 5. Stored state snapshot vs. the committed state root.
            snap = read_json(self.paths.state_path(h))
            if snap is None:
                entry["issues"].append({
                    "code": "state_missing",
                    "message": "状态快照缺失或无法解析",
                })
            else:
                try:
                    stored_state = WorldState.from_dict(snap)
                    if stored_state.root() != blk.header.state_root:
                        entry["issues"].append({
                            "code": "state_root",
                            "message": "状态根与快照不符（状态文件可能被篡改）",
                        })
                except Exception:  # noqa: BLE001
                    entry["issues"].append({
                        "code": "state_missing",
                        "message": "状态快照无法解析",
                    })

            # 6. On-disk block vs. the loaded in-memory chain (tamper check).
            if h <= self.height:
                mem = self.get_block(h)
                if mem is not None:
                    disk_hash = blk.recompute_hash()
                    if disk_hash != mem.hash:
                        entry["issues"].append({
                            "code": "disk_mismatch",
                            "message": "磁盘区块与节点已加载的不一致"
                                       "（文件可能被篡改）",
                        })

            if entry["issues"]:
                entry["status"] = "corrupt"
            report["blocks"].append(entry)
            prev_block = blk
            prev_broken = False
            view_blocks.append(blk)

        report["corrupt"] = [b["height"] for b in report["blocks"]
                             if b["status"] == "corrupt"]
        report["valid"] = not report["corrupt"]
        report["errors"] = [
            f"高度 {b['height']}: {issue['message']}"
            for b in report["blocks"] if b["status"] == "corrupt"
            for issue in b["issues"]
        ]
        return report

    def validate_full_chain(self):
        """Legacy summary view of :meth:`validate_chain_detailed`."""
        report = self.validate_chain_detailed()
        return {"valid": report["valid"], "checked": report["checked"],
                "errors": report["errors"]}

    # ==================================================================== #
    # Quarantine: isolate damaged blocks (reversible)
    # ==================================================================== #
    def quarantine_records(self, public=False):
        """All quarantine batches (newest last), including restored ones.

        ``public=True`` strips the internal ``txs`` payload (transactions
        harvested for pool re-admission) from each record.
        """
        data = read_json(self.paths.quarantine_index_path, {"batches": []})
        batches = data.get("batches", [])
        if not public:
            return batches
        out = []
        for b in batches:
            b = dict(b)
            b.pop("txs", None)
            out.append(b)
        return out

    def _save_quarantine_records(self, batches):
        atomic_write_json(self.paths.quarantine_index_path,
                          {"batches": batches})

    def quarantine_public_summary(self):
        """Active (un-restored) batches, for display in the block explorer."""
        out = []
        for b in self.quarantine_records():
            if b.get("restored"):
                continue
            out.append({
                "id": b["id"],
                "from_height": b["from_height"],
                "to_height": b["to_height"],
                "reason": b.get("reason", ""),
                "time": b.get("time"),
            })
        return out

    def quarantined_height(self, height):
        """Return the active batch isolating ``height``, if any."""
        for b in self.quarantine_public_summary():
            if b["from_height"] <= height <= b["to_height"]:
                return b
        return None

    def quarantine(self, height, reason="", issues=None):
        """Isolate blocks ``[height..]`` and roll the chain back if needed.

        Two situations are handled uniformly:

        * ``height`` is on the loaded main chain — the chain is truncated to
          ``height - 1`` (state restored from the snapshot there) and can
          keep browsing and mining from that point;
        * ``height`` is above the chain head — the chain was already
          truncated by a tolerant :meth:`load`, and the leftover (unloadable)
          block files on disk are isolated.

        In both cases the affected block/state files are *moved* into a
        quarantine batch directory (never deleted), so the operation can be
        undone later via :meth:`restore_quarantine`.  Transactions found in
        the isolated blocks are collected into ``batch["txs"]`` so the caller
        can re-admit them into the transaction pool.

        Returns ``(ok, message, batch)``.
        """
        if height <= 0:
            return False, "不允许隔离创世块", None
        disk_heights = self._disk_block_heights()
        max_disk = max(disk_heights + [-1])
        if height > max(self.height, max_disk):
            return False, (f"高度 {height} 超出当前链范围"
                           f"（链高 {self.height}）"), None
        rollback = height <= self.height
        if rollback and not os.path.exists(self.paths.state_path(height - 1)):
            return False, (f"高度 {height - 1} 的状态快照缺失，"
                           f"无法安全回滚到隔离点之前"), None

        head_height = max(self.height, max_disk)
        head_hash_before = self.head.hash if self.head else None
        batch_id = (time.strftime("q%Y%m%d-%H%M%S") + f"-h{height}-"
                    + uuid.uuid4().hex[:6])
        batch_dir = os.path.join(self.paths.quarantine_dir, batch_id)
        os.makedirs(os.path.join(batch_dir, "blocks"), exist_ok=True)
        os.makedirs(os.path.join(batch_dir, "state"), exist_ok=True)

        moved = []
        txs = []
        for h in range(height, head_height + 1):
            bp = self.paths.block_path(h)
            if os.path.exists(bp):
                # Best-effort: harvest transactions for pool re-admission
                # before the file disappears into the quarantine area.
                try:
                    data = read_json(bp)
                    if data:
                        blk = Block.from_dict(data)
                        txs.extend(tx.to_dict() for tx in blk.transactions
                                   if not tx.is_coinbase())
                except Exception:  # noqa: BLE001 - damaged file, skip
                    pass
                shutil.move(bp, os.path.join(batch_dir, "blocks",
                                             "%06d.json" % h))
                moved.append(h)
            sp = self.paths.state_path(h)
            if os.path.exists(sp):
                shutil.move(sp, os.path.join(batch_dir, "state",
                                             "%06d.json" % h))

        batch = {
            "id": batch_id,
            "from_height": height,
            "to_height": head_height,
            "heights": moved,
            "reason": reason or "",
            "issues": issues or [],
            "txs": txs,
            "time": time.time(),
            "head_hash_before": head_hash_before,
            "restored": False,
            "restored_at": None,
        }

        if rollback:
            # Roll the in-memory chain back to just before the gap.
            state_data = read_json(self.paths.state_path(height - 1))
            self.state = WorldState.from_dict(state_data)
            self.chain = self.chain[:height]
            self.chainwork = self.cumulative_work_of(self.chain)
            # Fork entries at/above the gap can never become main chain now.
            self.fork_store = {h: bs for h, bs in self.fork_store.items()
                               if h < height}
        self._write_meta()
        self.versions.record(self.height, self.head.hash,
                             meta={"op": "quarantine", "batch": batch_id})

        batches = self.quarantine_records()
        batches.append(batch)
        self._save_quarantine_records(batches)
        # The load-time problems this quarantine addresses are now handled.
        self.load_errors = [
            e for e in self.load_errors
            if e.get("height") is not None and e["height"] < height
        ]
        return True, (f"已隔离高度 {height}–{head_height} 共 {len(moved)} 个区块"
                      f"（批次 {batch_id}），链当前高度 {self.height}"), batch

    def restore_quarantine(self, batch_id):
        """Undo a quarantine batch: move its files back and reload the chain.

        Only possible while no new blocks have been mined over the gap.
        Restoring reinstates the exact pre-quarantine state — including any
        still-damaged files, which the next validation report will flag again.
        """
        batches = self.quarantine_records()
        batch = next((b for b in batches if b["id"] == batch_id), None)
        if batch is None:
            return False, "隔离批次不存在"
        if batch.get("restored"):
            return False, "该批次已被恢复过"
        if self.height >= batch["from_height"]:
            return False, (f"隔离位置（高度 {batch['from_height']}）之后已有"
                           f"新区块，请先回滚到高度 "
                           f"{batch['from_height'] - 1} 再撤销隔离")

        batch_dir = os.path.join(self.paths.quarantine_dir, batch_id)
        if not os.path.isdir(batch_dir):
            return False, "隔离区文件已丢失（批次目录不存在），无法恢复"
        for sub, target in (("blocks", self.paths.blocks_dir),
                            ("state", self.paths.state_dir)):
            src_dir = os.path.join(batch_dir, sub)
            if not os.path.isdir(src_dir):
                continue
            for f in os.listdir(src_dir):
                shutil.move(os.path.join(src_dir, f),
                            os.path.join(target, f))
        shutil.rmtree(batch_dir, ignore_errors=True)

        # Point meta back at the restored head and reload tolerantly: if the
        # restored files still contain a damaged block, the chain truncates
        # just before it — exactly the pre-quarantine situation.
        meta = read_json(self.paths.meta_path, {})
        meta["height"] = batch["to_height"]
        atomic_write_json(self.paths.meta_path, meta)
        self.load()

        batch["restored"] = True
        batch["restored_at"] = time.time()
        self._save_quarantine_records(batches)
        self.versions.record(self.height, self.head.hash,
                             meta={"op": "restore_quarantine",
                                   "batch": batch_id})
        return True, f"已撤销隔离批次 {batch_id}，链恢复到高度 {self.height}"
