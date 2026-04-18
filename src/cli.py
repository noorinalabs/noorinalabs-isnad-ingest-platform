"""CLI entry point for the isnad-graph-ingestion pipeline."""

from __future__ import annotations

import argparse
import sys


def _mask_password(value: str) -> str:
    """Replace all but first and last character with asterisks."""
    if len(value) <= 2:
        return "*" * len(value)
    return value[0] + "*" * (len(value) - 2) + value[-1]


def _check_neo4j() -> None:
    """Pre-flight Neo4j connectivity check. Exits with code 1 on failure."""
    from neo4j import GraphDatabase

    from src.config import get_settings

    settings = get_settings()
    print("Checking Neo4j connectivity...")
    try:
        driver = GraphDatabase.driver(
            settings.neo4j.uri,
            auth=(settings.neo4j.user, settings.neo4j.password),
        )
        driver.verify_connectivity()
        driver.close()
    except Exception as exc:
        print(f"ERROR: Cannot connect to Neo4j at {settings.neo4j.uri}: {exc}")
        sys.exit(1)
    print("  Neo4j is reachable.")


def _cmd_info() -> None:
    """Print configuration (masked passwords) and check DB connectivity."""
    from src.config import get_settings

    settings = get_settings()

    print("=== isnad-ingest configuration ===")
    print(f"  neo4j.uri      : {settings.neo4j.uri}")
    print(f"  neo4j.user     : {settings.neo4j.user}")
    print(f"  neo4j.password : {_mask_password(settings.neo4j.password)}")
    print(f"  postgres.dsn   : {settings.postgres.dsn}")
    print(f"  data_raw_dir   : {settings.data_raw_dir}")
    print(f"  data_staging_dir: {settings.data_staging_dir}")
    print(f"  data_curated_dir: {settings.data_curated_dir}")
    print(f"  log_level      : {settings.log_level}")
    print()

    print("=== connectivity ===")
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            settings.neo4j.uri,
            auth=(settings.neo4j.user, settings.neo4j.password),
        )
        driver.verify_connectivity()
        driver.close()
        print("  neo4j    : connected")
    except Exception:  # noqa: BLE001
        print("  neo4j    : unavailable")

    try:
        import psycopg

        conn = psycopg.connect(settings.postgres.dsn)
        conn.close()
        print("  postgres : connected")
    except Exception:  # noqa: BLE001
        print("  postgres : unavailable")


def _cmd_acquire() -> None:
    """Run all data acquisition downloaders."""
    from pathlib import Path

    from src.acquire import run_all as acquire_all
    from src.config import get_settings

    settings = get_settings()
    results = acquire_all(Path(settings.data_raw_dir))
    ok = sum(1 for v in results.values() if v)
    print(f"Acquisition complete. {ok}/{len(results)} sources downloaded.")


def _cmd_parse() -> None:
    """Run all parsers to produce staging Parquet files."""
    from pathlib import Path

    from src.config import get_settings
    from src.parse import run_all as parse_all

    settings = get_settings()
    results = parse_all(Path(settings.data_raw_dir), Path(settings.data_staging_dir))
    total_files = sum(len(v) for v in results.values())
    print(f"Parsing complete. {total_files} staging files produced.")


def _cmd_resolve() -> None:
    """Run the Phase 2 entity resolution pipeline."""
    from pathlib import Path

    from src.config import get_settings
    from src.resolve import run_all as resolve_all

    settings = get_settings()
    results = resolve_all(
        Path(settings.data_raw_dir),
        Path(settings.data_staging_dir),
        Path(settings.data_curated_dir),
    )
    total = sum(len(v) for v in results.values())
    print(f"\nResolution complete. {total} output files.")


def _cmd_load(
    *, skip_validation: bool = False, nodes_only: bool = False, incremental: bool = False
) -> None:
    """Run the Phase 3 graph loading pipeline."""
    import time
    from pathlib import Path

    from src.config import get_settings

    settings = get_settings()

    _check_neo4j()

    from src.graph import load_all
    from src.pipeline.audit import create_audit_entry, write_audit_entry
    from src.pipeline.manifest import (
        LAST_LOADED_MANIFEST_FILENAME,
        MANIFEST_FILENAME,
        compare_manifests,
        generate_manifest,
        load_manifest,
        save_manifest,
    )
    from src.utils.neo4j_client import Neo4jClient

    staging_dir = Path(settings.data_staging_dir)
    curated_dir = Path(settings.data_curated_dir)
    data_dir = staging_dir.parent
    queries_dir = Path("queries")

    start = time.monotonic()

    current_manifest = generate_manifest(data_dir)
    save_manifest(current_manifest, data_dir / MANIFEST_FILENAME)

    skipped_files: list[str] = []
    changed_file_details: list[dict[str, str]] = []

    if incremental:
        previous_manifest = load_manifest(data_dir / LAST_LOADED_MANIFEST_FILENAME)
        diff = compare_manifests(current_manifest, previous_manifest)
        if not diff.has_changes:
            print("No changes detected. Skipping incremental load.")
            return
        print(
            f"Incremental load: {len(diff.changed_files)} changed, "
            f"{len(diff.unchanged)} unchanged, {len(diff.removed)} removed."
        )
        skipped_files = diff.unchanged

        for f in diff.changed_files:
            entry: dict[str, str] = {"file": f, "md5_after": current_manifest[f]["md5"]}
            if f in previous_manifest:
                entry["md5_before"] = previous_manifest[f]["md5"]
            changed_file_details.append(entry)

    with Neo4jClient() as client:
        summary = load_all(
            client,
            staging_dir,
            curated_dir,
            queries_dir,
            strict=False,
            skip_validation=skip_validation,
            nodes_only=nodes_only,
            skip_files=skipped_files if incremental else None,
        )

    duration = time.monotonic() - start

    save_manifest(current_manifest, data_dir / LAST_LOADED_MANIFEST_FILENAME)

    audit = create_audit_entry(
        "load",
        duration_seconds=round(duration, 2),
        files_changed=changed_file_details,
        rows_affected=summary.total_nodes + summary.total_edges,
        summary={
            "total_nodes": summary.total_nodes,
            "total_edges": summary.total_edges,
            "incremental": incremental,
            "files_skipped": len(skipped_files),
        },
    )
    write_audit_entry(data_dir, audit)

    print("\n=== Load Summary ===")
    print(f"  Nodes loaded : {summary.total_nodes}")
    print(f"  Edges loaded : {summary.total_edges}")
    if incremental:
        print(f"  Files skipped: {len(skipped_files)}")

    for nr in summary.node_results:
        print(f"    {nr.node_type}: created={nr.created} merged={nr.merged} skipped={nr.skipped}")
    for er in summary.edge_results:
        print(
            f"    {er.edge_type}: created={er.created} skipped={er.skipped}"
            f" missing_endpoints={er.missing_endpoints}"
        )

    if summary.validation_results:
        print("\n=== Validation ===")
        for vr in summary.validation_results:
            status = "PASS" if vr.passed else "FAIL"
            print(f"  [{status}] {vr.query_name}: {vr.details}")
        if not summary.validation_passed:
            print("\nWARNING: Some validation checks failed.")
            sys.exit(1)
        else:
            print("\nAll validation checks passed.")


def _cmd_validate() -> None:
    """Run graph validation queries against an existing Neo4j database."""
    from pathlib import Path

    _check_neo4j()

    from src.graph.validate import run_validation
    from src.utils.neo4j_client import Neo4jClient

    queries_dir = Path("queries")

    with Neo4jClient() as client:
        results = run_validation(client, queries_dir)

    if not results:
        print("No validation queries found.")
        sys.exit(0)

    print("=== Validation Results ===")
    all_passed = True
    for vr in results:
        status = "PASS" if vr.passed else "FAIL"
        print(f"  [{status}] {vr.query_name}: {vr.details}")
        if not vr.passed:
            all_passed = False

    if not all_passed:
        print("\nWARNING: Some validation checks failed.")
        sys.exit(1)
    else:
        print("\nAll validation checks passed.")


def _cmd_enrich(
    *,
    only: list[str] | None = None,
    skip: list[str] | None = None,
    incremental: bool = False,
) -> None:
    """Run the Phase 4 enrichment pipeline."""
    import time
    from pathlib import Path

    from src.config import get_settings
    from src.enrich import run_all as enrich_all
    from src.pipeline.audit import create_audit_entry, write_audit_entry
    from src.pipeline.manifest import (
        LAST_LOADED_MANIFEST_FILENAME,
        MANIFEST_FILENAME,
        compare_manifests,
        generate_manifest,
        load_manifest,
    )
    from src.utils.neo4j_client import Neo4jClient

    settings = get_settings()
    _check_neo4j()

    staging_dir = Path(settings.data_staging_dir)
    data_dir = staging_dir.parent
    start = time.monotonic()

    affected_corpora: set[str] | None = None
    changed_file_details: list[dict[str, str]] = []

    if incremental:
        current_manifest = generate_manifest(data_dir)
        previous_manifest = load_manifest(data_dir / LAST_LOADED_MANIFEST_FILENAME)
        if not previous_manifest:
            previous_manifest = load_manifest(data_dir / MANIFEST_FILENAME)
        diff = compare_manifests(current_manifest, previous_manifest)
        if not diff.has_changes:
            print("No changes detected. Skipping incremental enrich.")
            return

        affected_corpora = set()
        for f in diff.changed_files:
            basename = f.rsplit("/", 1)[-1]
            parts = basename.replace(".parquet", "").split("_", 1)
            if len(parts) > 1:
                affected_corpora.add(parts[1])

            entry: dict[str, str] = {"file": f, "md5_after": current_manifest[f]["md5"]}
            if f in previous_manifest:
                entry["md5_before"] = previous_manifest[f]["md5"]
            changed_file_details.append(entry)

        print(
            f"Incremental enrich: {len(diff.changed_files)} files changed, "
            f"affected corpora: {', '.join(sorted(affected_corpora)) or 'none'}"
        )

    with Neo4jClient() as client:
        summary = enrich_all(
            client, staging_dir, only=only, skip=skip, affected_corpora=affected_corpora
        )

    duration = time.monotonic() - start

    audit = create_audit_entry(
        "enrich",
        duration_seconds=round(duration, 2),
        files_changed=changed_file_details,
        summary={
            "steps_completed": summary.steps_completed,
            "steps_failed": summary.steps_failed,
            "incremental": incremental,
            "affected_corpora": sorted(affected_corpora) if affected_corpora else [],
        },
    )
    write_audit_entry(data_dir, audit)

    print("\n=== Enrich Summary ===")
    print(f"  Steps completed: {', '.join(summary.steps_completed) or 'none'}")
    print(f"  Steps failed   : {', '.join(summary.steps_failed) or 'none'}")

    if summary.metrics:
        m = summary.metrics
        print(
            f"\n  Metrics: {m.narrators_enriched} narrators enriched, "
            f"{m.communities_found} communities"
        )
    if summary.topics:
        t = summary.topics
        print(f"  Topics: {t.hadiths_classified} classified, {t.hadiths_skipped} skipped")
    if summary.historical:
        h = summary.historical
        print(
            f"  Historical: {h.edges_created} edges, "
            f"{h.narrators_linked} narrators, {h.events_linked} events"
        )

    if summary.steps_failed:
        sys.exit(1)


def _cmd_audit(*, last_n: int = 10) -> None:
    """Display recent pipeline audit entries."""
    from pathlib import Path

    from src.config import get_settings
    from src.pipeline.audit import list_recent_entries

    settings = get_settings()
    data_dir = Path(settings.data_staging_dir).parent

    entries = list_recent_entries(data_dir, last_n=last_n)
    if not entries:
        print("No audit entries found.")
        return

    print(f"=== Last {len(entries)} Audit Entries ===\n")
    for entry in entries:
        print(f"  [{entry.stage}] {entry.timestamp}")
        print(f"    Duration: {entry.duration_seconds}s | Operator: {entry.operator}")
        if entry.files_changed:
            print(f"    Files changed: {len(entry.files_changed)}")
        if entry.rows_affected:
            print(f"    Rows affected: {entry.rows_affected}")
        if entry.summary:
            for k, v in entry.summary.items():
                print(f"    {k}: {v}")
        print()


def _cmd_reset(
    *,
    stage: str | None,
    source: str | None,
    full: bool,
    assume_yes: bool,
    dry_run: bool,
) -> None:
    """Execute a pipeline reset. Writes an audit entry for every run."""
    from pathlib import Path

    from src.config import get_settings
    from src.pipeline.reset import PipelineResetter, ResetScope

    # Build the scope first — validates inputs before any confirmation.
    provided = sum(1 for f in (stage, source, full) if f)
    if provided != 1:
        print("ERROR: reset requires exactly one of --stage, --source, --all.")
        sys.exit(2)

    if stage:
        scope = ResetScope.stage_scope(stage)
        summary_line = f"STAGE reset: prefix '{stage}/' and consumer offsets"
    elif source:
        scope = ResetScope.source_scope(source)
        summary_line = f"SOURCE reset: all stage prefixes for source '{source}'"
    else:
        scope = ResetScope.full_scope()
        summary_line = (
            "FULL reset: every pipeline B2 prefix, every pipeline Kafka topic, "
            "Neo4j hadith graph, PG hadith metadata. "
            "Users/roles/sessions are PRESERVED."
        )

    print(f"=== Pipeline Reset ===\n  {summary_line}\n")

    if scope.level == "full" and not assume_yes:
        confirm = input("Type 'OBLITERATE' to proceed: ").strip()
        if confirm != "OBLITERATE":
            print("Aborted.")
            sys.exit(1)

    if dry_run:
        print("Dry run — no changes were made. Scope validated.")
        return

    settings = get_settings()
    object_store, kafka_admin, neo4j, pg = _build_reset_clients(settings)

    resetter = PipelineResetter(
        object_store=object_store,
        kafka_admin=kafka_admin,
        neo4j=neo4j,
        pg=pg,
        data_dir=Path(settings.data_raw_dir).parent,
    )
    report, _entry, audit_path = resetter.reset(scope)

    print("\n=== Reset Complete ===")
    print(f"  Level              : {report.level}")
    print(f"  S3 prefixes deleted: {len(report.s3_prefixes_deleted)}")
    print(f"  S3 objects deleted : {report.s3_objects_deleted}")
    print(f"  Kafka topics reset : {len(report.kafka_topics_reset)}")
    if report.level == "full":
        print(f"  Neo4j rows deleted : {report.neo4j_rows_deleted}")
        print(f"  PG rows deleted    : {report.pg_rows_deleted}")
    print(f"  Duration (s)       : {report.duration_seconds}")
    print(f"  Audit entry        : {audit_path}")


def _build_reset_clients(settings: object) -> tuple[object, object, object, object]:
    """Construct the four reset adapters from settings. Isolated for testability."""
    # Imports kept inside the helper so unit tests that call _cmd_reset
    # via monkey-patched adapters don't drag in kafka/neo4j/boto3.
    import os

    import boto3  # type: ignore[import-untyped]
    import psycopg
    from kafka.admin import KafkaAdminClient  # type: ignore[import-untyped]
    from neo4j import GraphDatabase

    from src.pipeline.reset_adapters import (
        KafkaPythonAdmin,
        Neo4jHadithResetter,
        PgHadithMetadataResetter,
    )

    class _Store:
        def __init__(self, bucket: str, endpoint_url: str | None) -> None:
            self.bucket = bucket
            self._client = boto3.client(
                "s3", **({"endpoint_url": endpoint_url} if endpoint_url else {})
            )

        @property
        def client(self) -> object:
            return self._client

    bucket = os.environ.get("PIPELINE_BUCKET", "noorinalabs-pipeline")
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    neo4j_cfg = getattr(settings, "neo4j")
    pg_cfg = getattr(settings, "postgres")

    return (
        _Store(bucket=bucket, endpoint_url=endpoint_url),
        KafkaPythonAdmin(KafkaAdminClient(bootstrap_servers=bootstrap)),
        Neo4jHadithResetter(
            GraphDatabase.driver(neo4j_cfg.uri, auth=(neo4j_cfg.user, neo4j_cfg.password))
        ),
        PgHadithMetadataResetter(psycopg.connect(pg_cfg.dsn)),
    )


def _cmd_pipeline() -> None:
    """Run the full pipeline: acquire -> parse -> resolve -> load -> enrich."""
    print("=== Full Pipeline Run ===\n")

    print("--- Stage 1: Acquire ---")
    _cmd_acquire()
    print()

    print("--- Stage 2: Parse ---")
    _cmd_parse()
    print()

    print("--- Stage 3: Resolve ---")
    _cmd_resolve()
    print()

    print("--- Stage 4: Load ---")
    _cmd_load()
    print()

    print("--- Stage 5: Enrich ---")
    _cmd_enrich()
    print()

    print("=== Full Pipeline Complete ===")


def main() -> None:
    """Run the isnad-ingest CLI."""
    parser = argparse.ArgumentParser(description="isnad-ingest: Hadith Data Ingestion Pipeline")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("info", help="Show configuration and database status")
    subparsers.add_parser("acquire", help="Download data sources")
    subparsers.add_parser("parse", help="Parse raw data to staging")
    subparsers.add_parser("resolve", help="Entity resolution")

    load_parser = subparsers.add_parser("load", help="Load graph database")
    load_parser.add_argument(
        "--skip-validation", action="store_true", help="Skip validation queries after loading"
    )
    load_parser.add_argument(
        "--nodes-only", action="store_true", help="Load only nodes (skip edges and validation)"
    )
    load_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only load Parquet files whose hash changed since last load",
    )

    enrich_parser = subparsers.add_parser("enrich", help="Compute metrics and enrichment")
    enrich_parser.add_argument(
        "--only",
        nargs="+",
        choices=["metrics", "topics", "historical"],
        help="Run only these enrichment steps",
    )
    enrich_parser.add_argument(
        "--skip",
        nargs="+",
        choices=["metrics", "topics", "historical"],
        help="Skip these enrichment steps",
    )
    enrich_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only re-enrich data affected by changed Parquet files",
    )

    subparsers.add_parser("validate", help="Run graph validation queries")

    vs_parser = subparsers.add_parser("validate-staging", help="Validate staging Parquet files")
    vs_parser.add_argument(
        "--strict",
        action="store_true",
        help="Halt on any validation failure (default: warn mode)",
    )
    vs_parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Write JSON validation report to this path",
    )
    vs_parser.add_argument(
        "--drift-tolerance",
        type=float,
        default=30.0,
        help="Maximum drift percentage from baseline (default: 30.0)",
    )

    audit_parser = subparsers.add_parser("audit", help="View recent pipeline audit entries")
    audit_parser.add_argument(
        "--last",
        type=int,
        default=10,
        help="Number of recent audit entries to display (default: 10)",
    )

    subparsers.add_parser("pipeline", help="Run the full pipeline end-to-end")

    reset_parser = subparsers.add_parser(
        "reset",
        help="Reset pipeline state (stage, source, or full). Writes audit log.",
    )
    reset_group = reset_parser.add_mutually_exclusive_group(required=True)
    reset_group.add_argument(
        "--stage",
        choices=["raw", "dedup", "enriched", "normalized", "staged"],
        help="Wipe one B2 stage prefix + reset that stage's Kafka consumer offsets",
    )
    reset_group.add_argument(
        "--source",
        type=str,
        help="Wipe all stage prefixes for one data source (e.g. sunnah-api)",
    )
    reset_group.add_argument(
        "--all",
        action="store_true",
        help="Full reset: every prefix, every topic, Neo4j + PG hadith data (preserves users)",
    )
    reset_parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the 'OBLITERATE' confirmation for --all. Required for non-interactive runs.",
    )
    reset_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate scope and print what would be reset without touching anything.",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "info":
        _cmd_info()
    elif args.command == "acquire":
        _cmd_acquire()
    elif args.command == "parse":
        _cmd_parse()
    elif args.command == "resolve":
        _cmd_resolve()
    elif args.command == "load":
        _cmd_load(
            skip_validation=args.skip_validation,
            nodes_only=args.nodes_only,
            incremental=args.incremental,
        )
    elif args.command == "enrich":
        _cmd_enrich(only=args.only, skip=args.skip, incremental=args.incremental)
    elif args.command == "validate":
        _cmd_validate()
    elif args.command == "validate-staging":
        from pathlib import Path

        from src.config import get_settings
        from src.parse.validate import Strictness, validate_staging

        settings = get_settings()
        strictness = Strictness.STRICT if args.strict else Strictness.WARN
        output_json = Path(args.output_json) if args.output_json else None
        report = validate_staging(
            Path(settings.data_staging_dir),
            strictness=strictness,
            drift_tolerance_pct=args.drift_tolerance,
            output_json=output_json,
        )
        if not report.passed and strictness == Strictness.STRICT:
            print("\nValidation FAILED in strict mode. Pipeline halted.")
            sys.exit(1)
    elif args.command == "audit":
        _cmd_audit(last_n=args.last)
    elif args.command == "pipeline":
        _cmd_pipeline()
    elif args.command == "reset":
        _cmd_reset(
            stage=args.stage,
            source=args.source,
            full=args.all,
            assume_yes=args.yes,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
