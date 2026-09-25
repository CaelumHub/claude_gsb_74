"""Quarantine store: isolate damaged blocks without losing them.

When a block file is corrupt or tampered with, simply deleting it would make the
damage permanent and leave no way to recover.  Instead a *quarantine operation*
physically moves the affected block (and its state snapshot) out of the active
chain directory into ``quarantine/ops/<op-id>/`` while recording a manifest:

* ``quarantined`` blocks are the ones the validation report proved corrupt;
* ``detached`` blocks are the healthy blocks that followed them — they are not
  damaged themselves, but they cannot stay on the active chain once their
  ancestors are removed (the chain must remain a linked prefix).

Every operation is fully reversible: undoing it moves the original files back
into place.  If mining has since produced a replacement block at the same
height, the restore is refused rather than silently overwriting new data.
"""

import os
import shutil
import time
import uuid

from .storage import atomic_write_json, read_json

STATUS_QUARANTINED = "quarantined"   # proven corrupt / invalid, moved aside
STATUS_DETACHED = "detached"         # healthy but orphaned by a quarantine


class QuarantineStore:
    def __init__(self, paths):
        self.paths = paths
        self.paths.ensure()
        self.operations = read_json(paths.quarantine_ops_path, [])

    # ------------------------------------------------------------------ #
    # Persistence / queries
    # ------------------------------------------------------------------ #
    def _save(self):
        atomic_write_json(self.paths.quarantine_ops_path, self.operations)

    def list_operations(self):
        """All quarantine operations, newest first (undone ones marked)."""
        return sorted(self.operations, key=lambda o: o["time"], reverse=True)

    def get_operation(self, op_id):
        return next((o for o in self.operations if o["id"] == op_id), None)

    def active_operations(self):
        return [o for o in self.operations if o.get("active")]

    def active_blocks(self):
        """Map of height -> block record for every block still quarantined."""
        result = {}
        for op in self.active_operations():
            for rec in op["blocks"]:
                result[rec["height"]] = dict(rec, op_id=op["id"])
        return result

    def get_active_block(self, height):
        return self.active_blocks().get(height)

    def max_height(self):
        """Highest height known to the store (0 when empty)."""
        heights = [r["height"] for r in self.active_blocks()]
        return max(heights) if heights else 0

    # ------------------------------------------------------------------ #
    # On-disk layout of one operation
    # ------------------------------------------------------------------ #
    def _op_dir(self, op_id):
        return os.path.join(self.paths.quarantine_dir, "ops", op_id)

    def _file(self, op_id, status, kind, height):
        """Path a moved block/state file is stored at inside an operation."""
        sub = "quarantined" if status == STATUS_QUARANTINED else "detached"
        return os.path.join(self._op_dir(op_id), sub, kind, "%06d.json" % height)

    def block_file_of(self, rec):
        return self._file(rec["op_id"], rec["status"], "blocks", rec["height"])

    def state_file_of(self, rec):
        return self._file(rec["op_id"], rec["status"], "state", rec["height"])

    def read_block_data(self, height):
        """Raw JSON of a quarantined/detached block, or ``None``."""
        rec = self.get_active_block(height)
        if not rec:
            return None
        return read_json(self.block_file_of(rec))

    def get_block_hash(self, height):
        """Best-known hash for a quarantined height (may be ``None``)."""
        rec = self.get_active_block(height)
        return rec.get("hash") if rec else None

    # ------------------------------------------------------------------ #
    # Quarantine / restore
    # ------------------------------------------------------------------ #
    def create_operation(self, quarantined, detached, truncate_height, note=""):
        """Move the given blocks aside and record a reversible operation.

        ``quarantined`` / ``detached`` are lists of report entries (dicts with
        at least ``height``/``errors``/``reason``/``hash``).  Returns the stored
        operation manifest.  Callers are responsible for truncating the active
        chain state consistently (``truncate_height`` = last healthy height).
        """
        op_id = "q-" + time.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
        os.makedirs(self._op_dir(op_id), exist_ok=True)
        records = []
        moved = []          # (status, kind, height) of every file moved
        try:
            for status, entries in ((STATUS_QUARANTINED, quarantined),
                                    (STATUS_DETACHED, detached)):
                for entry in entries:
                    h = int(entry["height"])
                    rec = {
                        "height": h,
                        "status": status,
                        "reason": entry.get("reason", ""),
                        "errors": list(entry.get("errors", [])),
                        "hash": entry.get("hash"),
                        "block_file_present": False,
                        "state_file_present": False,
                    }
                    src_block = self.paths.block_path(h)
                    if os.path.exists(src_block):
                        dst = self._file(op_id, status, "blocks", h)
                        os.makedirs(os.path.dirname(dst), exist_ok=True)
                        shutil.move(src_block, dst)
                        rec["block_file_present"] = True
                        moved.append((status, "blocks", h))
                    src_state = self.paths.state_path(h)
                    if os.path.exists(src_state):
                        dst = self._file(op_id, status, "state", h)
                        os.makedirs(os.path.dirname(dst), exist_ok=True)
                        shutil.move(src_state, dst)
                        rec["state_file_present"] = True
                        moved.append((status, "state", h))
                    records.append(rec)
        except BaseException:
            # Best-effort rollback of files already moved so a failed
            # quarantine never leaves the chain half-partitioned.
            for status, kind, h in reversed(moved):
                src = self._file(op_id, status, kind, h)
                dst = (self.paths.block_path(h) if kind == "blocks"
                       else self.paths.state_path(h))
                if os.path.exists(src) and not os.path.exists(dst):
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.move(src, dst)
            raise

        records.sort(key=lambda r: r["height"])
        op = {
            "id": op_id,
            "time": time.time(),
            "active": True,
            "truncate_height": truncate_height,
            "note": note,
            "blocks": records,
        }
        self.operations.append(op)
        self._save()
        return op

    def undo_operation(self, op_id, expected_tip=None):
        """Move all files of an operation back onto the active chain.

        Returns ``(ok, message, restored_heights)``.  Refuses (moving nothing)
        when a target file already exists — typically because mining has since
        produced replacement blocks — or when ``expected_tip`` is given and the
        current chain tip does not match the truncation point recorded for the
        operation (the chain history has since diverged).
        """
        op = self.get_operation(op_id)
        if op is None:
            return False, f"隔离操作 {op_id} 不存在", []
        if not op.get("active"):
            return False, f"隔离操作 {op_id} 已撤销", []

        if expected_tip is not None and expected_tip != \
                op.get("truncate_height"):
            return False, (f"当前链头（#{expected_tip}）与隔离时的截断点"
                           f"（#{op.get('truncate_height')}）不一致，"
                           f"链历史已分叉，无法整体恢复"), []

        # Pre-flight: every existing target must be free, otherwise abort
        # before touching anything.
        conflicts = []
        for rec in op["blocks"]:
            h = rec["height"]
            if rec.get("block_file_present") and \
                    os.path.exists(self.paths.block_path(h)):
                conflicts.append(h)
        if conflicts:
            heights = ", ".join("#%d" % h for h in sorted(conflicts))
            return False, (f"高度 {heights} 已存在新区块文件，恢复会覆盖新数据；"
                           f"请先回滚这些区块再撤销隔离"), []

        restored = []
        for rec in sorted(op["blocks"], key=lambda r: r["height"], reverse=True):
            h = rec["height"]
            if rec.get("block_file_present"):
                src = self.block_file_of(dict(rec, op_id=op_id))
                if os.path.exists(src):
                    os.makedirs(self.paths.blocks_dir, exist_ok=True)
                    shutil.move(src, self.paths.block_path(h))
            if rec.get("state_file_present"):
                src = self.state_file_of(dict(rec, op_id=op_id))
                if os.path.exists(src):
                    os.makedirs(self.paths.state_dir, exist_ok=True)
                    shutil.move(src, self.paths.state_path(h))
            restored.append(h)

        op["active"] = False
        op["undone_at"] = time.time()
        self._save()
        # Remove the now-empty operation folder.
        shutil.rmtree(self._op_dir(op_id), ignore_errors=True)
        return True, f"已恢复 {len(restored)} 个区块", sorted(restored)

    def prune_empty(self):
        """Drop on-disk folders for operations whose manifest is gone."""
        ops_dir = os.path.join(self.paths.quarantine_dir, "ops")
        if not os.path.isdir(ops_dir):
            return
        known = {o["id"] for o in self.operations if o.get("active")}
        for name in os.listdir(ops_dir):
            if name not in known:
                shutil.rmtree(os.path.join(ops_dir, name), ignore_errors=True)
