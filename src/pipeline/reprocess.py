"""Reprocess-from-stage — re-run the pipeline from a chosen stage (issue #76).

Reprocessing from stage *X* re-delivers *X*'s input and lets the pipeline
flow forward from there, **without deleting any staged B2 data** (that is
what the reset surface is for). It is the lighter, non-destructive sibling
of ``src/pipeline/reset.py``.

Two things have to happen for a genuine reprocess:

1. **Rewind X's consumer group to the start of its consume topic** so the
   stage re-reads every batch it has already seen and re-emits downstream.
   Downstream stages then re-consume naturally as the re-emitted messages
   arrive at the tail of their topics — so only the *entry* stage's group
   is rewound, not every downstream group.
2. **Clear the idempotency checkpoints for X and every downstream stage.**
   Each worker guards against duplicate work with a ``(stage, batch_id)``
   checkpoint (``workers.lib.checkpoint_pg``). A re-delivered batch carries
   its original ``batch_id``, so without clearing those rows every stage
   from X onward would treat the re-delivery as already-done and skip it —
   making the reprocess a silent no-op. The checkpoint stage key equals the
   stage's ``consumer_group`` (see ``src/pipeline/stages.py``).

Every reprocess (dry-run included) writes an audit entry to ``data/audit/``
via ``src/pipeline/audit.py``, mirroring the reset surface, so operators
have a record of who re-ran what and when.

All resource clients (Kafka admin, checkpoint store) are injected so the
unit suite exercises the orchestration against in-memory fakes — no Kafka,
no Postgres.

Operational note: like the reset surface's offset reset, rewinding a
consumer group's committed offsets takes effect for the running workers on
their next rebalance/restart — Kafka only lets an offset be altered for a
group with no active members on that partition. The admin panel triggers
the rewind + checkpoint clear here; the worker fleet picks it up. This is
the same operational contract the reset endpoints already carry.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from src.pipeline.audit import AuditEntry, create_audit_entry, write_audit_entry
from src.pipeline.stages import STAGE_BY_NAME, StageSpec, downstream_stages

__all__ = [
    "CheckpointResetter",
    "OffsetRewinder",
    "PipelineReprocessor",
    "ReprocessPlan",
    "ReprocessReport",
    "plan_reprocess",
    "write_dry_run_audit",
]


class OffsetRewinder(Protocol):
    """Kafka surface needed to rewind a stage's consumer group.

    Matches ``src.pipeline.reset.KafkaAdmin.reset_consumer_offsets`` so the
    same ``KafkaPythonAdmin`` adapter satisfies both surfaces — rewinding to
    the start of the topic re-delivers every batch to the stage.
    """

    def reset_consumer_offsets(self, topic: str, group_id: str) -> None: ...


class CheckpointResetter(Protocol):
    """Idempotency-checkpoint surface. ``clear`` deletes every checkpoint row
    for one stage key and returns the number of rows removed."""

    def clear(self, stage_key: str) -> int: ...


@dataclass(frozen=True)
class ReprocessPlan:
    """Resolved, inspectable plan for a reprocess — what *would* run.

    Shared by the dry-run preview and the executed report so the admin UI
    sees the exact same affected stages / rewound group / topic in both.
    """

    from_stage: str
    rewound_group: str
    rewound_topic: str
    affected_stages: list[str]

    def describe(self, *, indent: str = "  ") -> str:
        lines = [
            f"REPROCESS from stage '{self.from_stage}':",
            f"{indent}Rewind consumer group : {self.rewound_group}",
            f"{indent}On consume topic       : {self.rewound_topic}",
            f"{indent}Clear checkpoints for  :",
        ]
        lines.extend(f"{indent}  - {s}" for s in self.affected_stages)
        return "\n".join(lines)


@dataclass
class ReprocessReport:
    """What a reprocess actually did. Saved into the audit-entry summary."""

    from_stage: str
    rewound_group: str
    rewound_topic: str
    affected_stages: list[str] = field(default_factory=list)
    checkpoints_cleared: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0
    dry_run: bool = False

    def to_summary(self) -> dict[str, Any]:
        return {
            "from_stage": self.from_stage,
            "rewound_group": self.rewound_group,
            "rewound_topic": self.rewound_topic,
            "affected_stages": self.affected_stages,
            "checkpoints_cleared": self.checkpoints_cleared,
            "dry_run": self.dry_run,
        }


def plan_reprocess(stage_name: str) -> ReprocessPlan:
    """Resolve the reprocess plan for ``stage_name``.

    Raises ``ValueError`` for an unknown stage (the API maps that to a 422).
    """
    stage = _resolve_stage(stage_name)
    affected = downstream_stages(stage.name)
    return ReprocessPlan(
        from_stage=stage.name,
        rewound_group=stage.consumer_group,
        rewound_topic=stage.consume_topic,
        affected_stages=[s.name for s in affected],
    )


def _resolve_stage(stage_name: str) -> StageSpec:
    stage = STAGE_BY_NAME.get(stage_name)
    if stage is None:
        raise ValueError(f"unknown stage: {stage_name!r}. Known: {sorted(STAGE_BY_NAME)}")
    return stage


class PipelineReprocessor:
    """Coordinates the offset rewind + checkpoint clear and writes an audit entry.

    Dependencies are injected so tests exercise the full flow against fakes —
    no Kafka, no Postgres.
    """

    def __init__(
        self,
        *,
        kafka: OffsetRewinder,
        checkpoints: CheckpointResetter,
        data_dir: Path,
    ) -> None:
        self.kafka = kafka
        self.checkpoints = checkpoints
        self.data_dir = data_dir

    def reprocess(self, stage_name: str) -> tuple[ReprocessReport, AuditEntry, Path]:
        """Rewind the stage's group, clear affected checkpoints, write audit.

        Raises ``ValueError`` for an unknown stage name.
        """
        stage = _resolve_stage(stage_name)
        affected = downstream_stages(stage.name)

        start = time.monotonic()

        # Clear checkpoints for the entry stage and every downstream stage
        # FIRST, so a re-delivered batch is never skipped between the rewind
        # and the clear. The checkpoint stage key is the consumer_group.
        cleared: dict[str, int] = {}
        for s in affected:
            cleared[s.name] = self.checkpoints.clear(s.consumer_group)

        # Then rewind only the entry stage's group to re-deliver its input.
        self.kafka.reset_consumer_offsets(stage.consume_topic, stage.consumer_group)

        report = ReprocessReport(
            from_stage=stage.name,
            rewound_group=stage.consumer_group,
            rewound_topic=stage.consume_topic,
            affected_stages=[s.name for s in affected],
            checkpoints_cleared=cleared,
            duration_seconds=round(time.monotonic() - start, 3),
        )
        entry = create_audit_entry(
            stage=f"reprocess-{stage.name}",
            duration_seconds=report.duration_seconds,
            rows_affected=sum(cleared.values()),
            summary=report.to_summary(),
        )
        path = write_audit_entry(self.data_dir, entry)
        return report, entry, path


def write_dry_run_audit(
    stage_name: str,
    *,
    data_dir: Path,
) -> tuple[ReprocessReport, AuditEntry, Path]:
    """Record a reprocess dry-run as an audit entry without doing any work.

    Mirrors the reset surface's dry-run: the preview the admin UI calls
    first must touch no infra, so this is kept module-level (no Kafka/PG
    adapters constructed) and writes an audit entry with ``dry_run=True`` and
    zeroed clear counts. Raises ``ValueError`` for an unknown stage name.
    """
    plan = plan_reprocess(stage_name)
    report = ReprocessReport(
        from_stage=plan.from_stage,
        rewound_group=plan.rewound_group,
        rewound_topic=plan.rewound_topic,
        affected_stages=plan.affected_stages,
        checkpoints_cleared={s: 0 for s in plan.affected_stages},
        dry_run=True,
    )
    entry = create_audit_entry(
        stage=f"reprocess-{plan.from_stage}",
        duration_seconds=0.0,
        rows_affected=0,
        summary=report.to_summary(),
    )
    path = write_audit_entry(data_dir, entry)
    return report, entry, path
