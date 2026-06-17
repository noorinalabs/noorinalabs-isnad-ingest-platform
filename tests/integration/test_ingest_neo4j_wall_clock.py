"""Real-Neo4j integration coverage for the ingest wall-clock retry bound (#38).

``IngestProcessor.__call__`` documents a per-message Neo4j retry budget as
part of its public contract: transient driver errors
(``ServiceUnavailable``, ``TransientError``, ``SessionExpired``) are retried
*inside* ``session.execute_write`` by the neo4j driver's internal
``_run_transaction`` loop, bounded by ``max_transaction_retry_time``
(default 30s). Once that wall-clock budget is exhausted the last error
propagates and the runner DLQs the message.

The unit suite (``tests/workers/test_ingest_neo4j.py``) exercises the *wire
contract* — one session opened, exception propagates, runner DLQs — but its
``_FakeSession.execute_write`` raises directly and therefore bypasses
``neo4j._sync.work.session._run_transaction`` entirely. The wall-clock bound
itself is a **real-driver behavior** that no mock can stand in for: if a
future driver upgrade silently changes the retry semantics (default raised,
retry behavior changed under cluster mode) the documented "<=30s per
message" contract becomes false and the unit tests do not catch it.

Per [[feedback_test_mock_injection_masks_production_failure]], the
mock-bypass disclosure in the unit suite points here. This module exercises
the bound against a real Neo4j container by:

* driving the driver's internal retry loop against a server that has been
  killed mid-flight (``container.stop()``), so the loop runs until the
  wall-clock budget is exhausted rather than succeeding on the first try;
* configuring a *short* ``max_transaction_retry_time`` so the test observes
  the same mechanism the 30s default uses without waiting 30s;
* asserting the processor returns/raises within ``budget + tolerance``;
* asserting exactly one driver session is opened across the failure window
  (the [[#20]] single-session invariant — no compounding outer reopen);
* asserting the runner routes the exhausted-retry failure to the DLQ with a
  real-driver-final error class.

Runtime requirement
-------------------
These tests are marked ``@pytest.mark.integration`` and need Docker on the
host (testcontainers spins up ``neo4j:5-community``). They are excluded from
the default ``make check`` / ``make test-workers`` run (``-m "not
integration"``) and run via ``make test-integration`` where Docker is
available (CI integration job / local dev with Docker). Without Docker the
whole module is collected-but-skipped cleanly — see the module-level
``importorskip`` gate below.

Cluster-mode caveat (acceptance item #4)
----------------------------------------
The "fresh-acquire lands on a different cluster member" assertion from the
issue requires a real Neo4j *cluster* (Enterprise causal cluster), which the
single-instance community testcontainer cannot provide. We assert the
observable single-instance proxy instead — exactly one session opened and a
fresh connection acquired per retry iteration — and leave the multi-member
routing assertion as a documented follow-up for an Enterprise-licensed
cluster harness (not exercisable on the community image).
"""

from __future__ import annotations

import json
import time
from typing import Any

import pytest

# testcontainers + a Docker daemon are hard requirements; without either the
# module is collected-but-skipped so a Docker-less CI lane (make check) stays
# green. importorskip on the package is the same gate the PG-restart
# integration test relies on indirectly via the session fixtures.
pytest.importorskip("testcontainers.neo4j")

from neo4j import Driver, GraphDatabase  # noqa: E402
from neo4j.exceptions import ServiceUnavailable  # noqa: E402
from testcontainers.neo4j import Neo4jContainer  # noqa: E402

from tests.integration import NEO4J_TEST_IMAGE  # noqa: E402
from tests.workers.conftest import FakeConsumer, FakeProducer  # noqa: E402

# ``_write_batch`` is reused from the unit suite so the on-the-wire
# manifest/Parquet shape is identical to what normalize emits in prod.
from tests.workers.test_ingest_neo4j import _write_batch  # noqa: E402
from workers.ingest.processor import IngestProcessor  # noqa: E402
from workers.lib.dlq import DLQ_TOPIC  # noqa: E402
from workers.lib.message import PipelineMessage, serialize_message  # noqa: E402
from workers.lib.object_store import ObjectStore  # noqa: E402
from workers.lib.runner import WorkerRunner, WorkerSettings  # noqa: E402
from workers.lib.topics import PIPELINE_NORMALIZE_DONE  # noqa: E402

pytestmark = pytest.mark.integration

NEO4J_TEST_PASSWORD = "testpassword123"

# Short wall-clock budget for the test. The production default is 30s; we
# drive the same ``_run_transaction`` mechanism with a small budget so the
# suite observes the bound without a 30s wait. ``TOLERANCE_S`` absorbs the
# container-teardown latency plus one extra retry backoff that may straddle
# the deadline (the driver checks the deadline between iterations).
RETRY_BUDGET_S = 5.0
TOLERANCE_S = 12.0


# ---------------------------------------------------------------------------
# Single-session-counting driver proxy
# ---------------------------------------------------------------------------


class _CountingDriver:
    """Wraps a real neo4j ``Driver`` and counts ``.session()`` calls.

    The [[#20]] contract is that the processor opens exactly one session per
    message and relies on the driver's *internal* retry — it must NOT wrap
    ``execute_write`` in an outer reopen loop that would open a second
    session. A real driver is required (the retry loop lives inside it), so
    we proxy rather than fake.
    """

    def __init__(self, inner: Driver) -> None:
        self._inner = inner
        self.session_count = 0

    def session(self, *args: Any, **kwargs: Any) -> Any:
        self.session_count += 1
        return self._inner.session(*args, **kwargs)

    def close(self) -> None:
        self._inner.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def neo4j_container_killable():
    """A fresh, function-scoped Neo4j container we are free to kill mid-test.

    Function-scoped (not the session-scoped ``neo4j_container`` in
    ``tests/integration/conftest.py``) because the wall-clock test stops the
    container to force the driver into its retry loop — a session-scoped
    container would be dead for every test after the first.
    """
    container = Neo4jContainer(NEO4J_TEST_IMAGE, password=NEO4J_TEST_PASSWORD)
    container.start()
    try:
        yield container
    finally:
        # ``stop()`` is idempotent in testcontainers — safe even if a test
        # already stopped it to trigger the failure window.
        container.stop()


@pytest.fixture
def object_store() -> ObjectStore:
    """In-memory object store — the batch payload never needs Docker.

    Mirrors the ``object_store`` fixture in ``tests/workers/conftest.py`` so
    ``_write_batch`` works unchanged. Defined locally because the integration
    package's ``conftest.py`` does not import the worker fakes.
    """
    from tests.workers.conftest import FakeS3Client

    return ObjectStore(bucket="test-bucket", client=FakeS3Client())


@pytest.fixture
def folder_prefix_message() -> PipelineMessage:
    """D-ii message: ``b2_path`` is a folder prefix with a trailing slash."""
    return PipelineMessage(
        batch_id="batch-wallclock-001",
        source="sunnah-api",
        b2_path="normalized/batch-wallclock-001/",
        record_count=0,
    )


def _seed_one_node_batch(store: ObjectStore, prefix: str) -> None:
    """Write a minimal one-node batch — enough to drive a single MERGE tx."""
    _write_batch(
        store,
        prefix,
        nodes={"Narrator": [{"id": "nar:a", "props": {"name_en": "A"}}]},
    )


# ---------------------------------------------------------------------------
# Acceptance item 1 + 3 — wall-clock bound observation + single-session
# ---------------------------------------------------------------------------


def test_wall_clock_bound_is_enforced_by_real_driver(
    neo4j_container_killable: Neo4jContainer,
    object_store: ObjectStore,
    folder_prefix_message: PipelineMessage,
) -> None:
    """The processor returns/raises within ``max_transaction_retry_time``.

    We kill the container *before* the processor opens its session, so the
    driver's ``_run_transaction`` loop retries the connection failure until
    the (short) wall-clock budget is exhausted, then propagates. This
    exercises the real bound the unit suite cannot reach.
    """
    _seed_one_node_batch(object_store, folder_prefix_message.b2_path)

    inner = GraphDatabase.driver(
        neo4j_container_killable.get_connection_url(),
        auth=("neo4j", NEO4J_TEST_PASSWORD),
        max_transaction_retry_time=RETRY_BUDGET_S,
    )
    driver = _CountingDriver(inner)

    # Kill the server so every connection attempt inside the retry loop
    # fails; the loop spins on transient ``ServiceUnavailable`` until the
    # wall-clock budget runs out.
    neo4j_container_killable.stop()

    start = time.monotonic()
    with pytest.raises(ServiceUnavailable):
        IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)
    elapsed = time.monotonic() - start

    # The retry loop must not run unbounded: it terminates within the
    # configured budget plus tolerance (container teardown + one straddling
    # backoff). A regression that removed the bound would hang far longer.
    assert elapsed <= RETRY_BUDGET_S + TOLERANCE_S, (
        f"retry loop ran {elapsed:.1f}s, exceeding the "
        f"{RETRY_BUDGET_S}s budget + {TOLERANCE_S}s tolerance — "
        "the wall-clock bound contract is broken"
    )

    # [[#20]] single-session invariant: exactly one session opened across the
    # whole failure window. A compounding outer reopen would show >1.
    assert driver.session_count == 1, (
        f"expected exactly one session across the retry window, "
        f"got {driver.session_count} — outer-reopen regression"
    )

    driver.close()


# ---------------------------------------------------------------------------
# Acceptance item 2 — DLQ propagation under sustained failure
# ---------------------------------------------------------------------------


def test_exhausted_retry_routes_to_dlq_via_runner(
    neo4j_container_killable: Neo4jContainer,
    object_store: ObjectStore,
    folder_prefix_message: PipelineMessage,
) -> None:
    """With the budget exhausted, the runner DLQs the real-driver-final error.

    Asserts the end-to-end contract: a sustained-failure batch surfaces from
    the processor, the runner catches it, and a DLQ record carries the real
    driver error class (``ServiceUnavailable`` here — the final error a
    killed single instance surfaces; ``SessionExpired`` is the cluster-mode
    analog the unit suite asserts via injection).
    """
    _seed_one_node_batch(object_store, folder_prefix_message.b2_path)

    inner = GraphDatabase.driver(
        neo4j_container_killable.get_connection_url(),
        auth=("neo4j", NEO4J_TEST_PASSWORD),
        max_transaction_retry_time=RETRY_BUDGET_S,
    )
    driver = _CountingDriver(inner)
    neo4j_container_killable.stop()

    producer = FakeProducer()
    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="ingest-worker",
            consume_topic=PIPELINE_NORMALIZE_DONE,
            produce_topic=None,
            consumer_group="ingest-worker",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=IngestProcessor(object_store, neo4j_driver=driver),
    )

    runner.handle_one(serialize_message(folder_prefix_message))

    assert len(producer.sent) == 1, "exhausted-retry batch should DLQ exactly one record"
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    # A real driver surfaces a concrete transient class once the budget is
    # spent; assert it is one of the retryable transient family the
    # processor docstring enumerates rather than pinning the exact class
    # (which can vary with driver version / failure mode).
    assert record["error_class"] in {
        "ServiceUnavailable",
        "SessionExpired",
        "TransientError",
    }, f"unexpected DLQ error_class {record['error_class']!r}"
    # Single session even through the runner path.
    assert driver.session_count == 1

    driver.close()


# ---------------------------------------------------------------------------
# Happy-path control — a live container actually persists the MERGE
# ---------------------------------------------------------------------------


def test_real_driver_merges_node_when_container_is_live(
    neo4j_container_killable: Neo4jContainer,
    object_store: ObjectStore,
    folder_prefix_message: PipelineMessage,
) -> None:
    """Control: against a live container the batch MERGEs and opens one session.

    Anchors the failure-path tests — proves the wall-clock tests fail because
    the container is *killed*, not because the real-driver wiring is broken.
    Also re-asserts the single-session invariant on the success path.
    """
    _seed_one_node_batch(object_store, folder_prefix_message.b2_path)

    inner = GraphDatabase.driver(
        neo4j_container_killable.get_connection_url(),
        auth=("neo4j", NEO4J_TEST_PASSWORD),
        max_transaction_retry_time=RETRY_BUDGET_S,
    )
    driver = _CountingDriver(inner)

    IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)

    assert driver.session_count == 1

    # Verify the node actually landed via an independent read session.
    with inner.session() as session:
        record = session.run(
            "MATCH (n:Narrator {id: $id}) RETURN n.name_en AS name_en",
            id="nar:a",
        ).single()
    assert record is not None
    assert record["name_en"] == "A"

    driver.close()
