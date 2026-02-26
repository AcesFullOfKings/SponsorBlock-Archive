#!/usr/bin/env python3
"""
Persona Segment Deleter for SponsorBlock Daily Databases

Removes all segments submitted by the "Persona" spammer from daily database files.
Cross-references staticData.sqlite3 to identify persona's segment IDs, then deletes
matching rows from each daily segmentData file.

Usage:
    python delete_persona_segments.py [options]

Options:
    --dry-run               Count deletions without actually deleting
    --start-from <DATE>     Resume from a specific date (YYYY-MM-DD)
    --vacuum                Run VACUUM after each deletion to reclaim disk space

Example:
    python delete_persona_segments.py --dry-run
    python delete_persona_segments.py --start-from 2025-06-01
"""

import sqlite3
import sys
import time
import re
import argparse
from pathlib import Path
from typing import Set, List, Tuple

# ============================================================================
# CONFIGURABLE PATHS — adjust these for your environment
# ============================================================================
STATIC_DB_PATH = Path(__file__).parent.parent / "archive" / "staticData.sqlite3"
DAILY_FILES_DIR = Path(__file__).parent.parent / "archive" / "Daily Files"
PERSONA_FILE = Path(__file__).parent / "all_personabots.txt"
# ============================================================================


def load_persona_hashes(filepath: Path) -> Set[str]:
    """Load persona bot user hashes from text file. Returns set of strings."""
    if not filepath.exists():
        raise FileNotFoundError(f"Persona hashes file not found: {filepath}")

    with open(filepath, 'r') as f:
        hashes = set(line.strip() for line in f if line.strip())

    print(f"Loaded {len(hashes):,} persona hashes from {filepath.name}")
    return hashes


def find_persona_ids(static_db_path: Path, persona_hashes: Set[str]) -> Tuple[Set[int], Set[int]]:
    """Query staticData to find all user and segment short_ids belonging to persona.

    Opens staticData in read-only mode. Uses an in-memory attached database for
    the temporary table so that staticData is never modified.

    Returns:
        Tuple of (persona_user_ids, persona_segment_ids) as sets of integers.
    """
    if not static_db_path.exists():
        raise FileNotFoundError(f"Static database not found: {static_db_path}")

    start_time = time.time()

    # Open read-only
    conn = sqlite3.connect(f"file:{static_db_path}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA cache_size = -64000")
        conn.execute("PRAGMA temp_store = MEMORY")

        # Attach in-memory db for temp table (no writes to staticData)
        conn.execute("ATTACH ':memory:' AS tmp")
        conn.execute("CREATE TABLE tmp.persona_hashes (long_id TEXT PRIMARY KEY)")

        # Batch insert persona hashes with progress
        print("Loading persona hashes into temporary table...")
        hash_list = list(persona_hashes)
        BATCH = 50000
        for i in range(0, len(hash_list), BATCH):
            batch = [(h,) for h in hash_list[i:i + BATCH]]
            conn.executemany("INSERT INTO tmp.persona_hashes VALUES (?)", batch)
            loaded = min(i + BATCH, len(hash_list))
            print(f"  [{loaded:,}/{len(hash_list):,}]")

        # Find matching user short_ids
        cursor = conn.execute("""
            SELECT u.short_id FROM tmp.persona_hashes ph
            JOIN users u ON u.long_id = ph.long_id
        """)
        persona_user_ids = set(row[0] for row in cursor)
        print(f"Found {len(persona_user_ids):,} persona users in staticData "
              f"(out of {len(persona_hashes):,} hashes)")

        # Find all their segments
        print("Finding persona segments (scanning segments table, may take a few minutes)...")
        cursor = conn.execute("""
            SELECT s.short_id
            FROM segments s
            WHERE s.user_id IN (SELECT short_id FROM tmp.persona_hashes ph
                                JOIN users u ON u.long_id = ph.long_id)
        """)

        persona_segment_ids = set(row[0] for row in cursor)

        elapsed = time.time() - start_time
        print(f"Found {len(persona_segment_ids):,} persona segments ({elapsed:.1f}s)")

        return persona_user_ids, persona_segment_ids

    finally:
        conn.close()


def get_daily_files(daily_dir: Path, start_from: str = None) -> List[Path]:
    """Find all daily database files, sorted chronologically.

    Args:
        daily_dir: Directory containing daily database files
        start_from: Optional YYYY-MM-DD date to start from (inclusive)

    Returns:
        Sorted list of Path objects
    """
    if not daily_dir.exists():
        raise FileNotFoundError(f"Daily files directory not found: {daily_dir}")

    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}_segmentData\.sqlite3$')
    files = [f for f in daily_dir.iterdir() if f.is_file() and pattern.match(f.name)]
    files.sort(key=lambda p: p.name)

    if start_from:
        start_prefix = start_from + "_"
        files = [f for f in files if f.name >= start_prefix]
        print(f"Resuming from {start_from} ({len(files)} files remaining)")

    if not files:
        print("No daily database files found.")

    return files


def process_single_daily_file(daily_file: Path, segment_id_tuples: List[Tuple[int]],
                              dry_run: bool, vacuum: bool) -> int:
    """Delete persona segments from a single daily file.

    Args:
        daily_file: Path to the daily database file
        segment_id_tuples: Pre-computed list of (segment_id,) tuples
        dry_run: If True, count without deleting
        vacuum: If True, VACUUM after deletion

    Returns:
        Number of rows deleted (or that would be deleted in dry-run mode)
    """
    conn = sqlite3.connect(daily_file)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")
        conn.execute("PRAGMA temp_store = MEMORY")

        # Row count before
        before_count = conn.execute("SELECT COUNT(*) FROM segment_data").fetchone()[0]

        # Create temp table with persona segment IDs
        conn.execute("CREATE TEMP TABLE persona_ids (id INTEGER PRIMARY KEY)")
        conn.executemany("INSERT INTO persona_ids VALUES (?)", segment_id_tuples)

        if dry_run:
            deleted = conn.execute("""
                SELECT COUNT(*) FROM segment_data
                WHERE segment_id IN (SELECT id FROM persona_ids)
            """).fetchone()[0]
        else:
            conn.execute("""
                DELETE FROM segment_data
                WHERE segment_id IN (SELECT id FROM persona_ids)
            """)
            deleted = conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()

            if vacuum and deleted > 0:
                print(f"  Running VACUUM...")
                conn.execute("VACUUM")

        after_count = before_count - deleted
        print(f"  Rows: {before_count:,} -> {after_count:,}")

        return deleted

    finally:
        conn.close()


def process_daily_files(daily_files: List[Path], persona_segment_ids: Set[int],
                        dry_run: bool = False, vacuum: bool = False):
    """Delete persona segments from all daily files.

    Args:
        daily_files: List of daily database file paths
        persona_segment_ids: Set of segment short_ids to delete
        dry_run: If True, count without deleting
        vacuum: If True, VACUUM after each deletion
    """
    total_files = len(daily_files)
    total_deleted = 0
    failed_files = []
    overall_start = time.time()

    # Pre-compute tuple list for batch insertion into temp tables
    segment_id_tuples = [(sid,) for sid in persona_segment_ids]

    mode_label = "DRY RUN" if dry_run else "LIVE"
    print(f"\nProcessing {total_files} daily files ({mode_label})")
    print("-" * 60)

    for i, daily_file in enumerate(daily_files, 1):
        print(f"\n[{i}/{total_files}] {daily_file.name}")
        file_start = time.time()

        try:
            deleted = process_single_daily_file(
                daily_file, segment_id_tuples, dry_run, vacuum)
            total_deleted += deleted

            elapsed = time.time() - file_start
            action = "Would delete" if dry_run else "Deleted"
            print(f"  {action} {deleted:,} rows ({elapsed:.1f}s)")

        except Exception as e:
            print(f"  ERROR: {e}")
            failed_files.append((daily_file.name, str(e)))

    # Summary
    overall_elapsed = time.time() - overall_start
    print("\n" + "=" * 60)
    print("DAILY FILES SUMMARY")
    print("=" * 60)
    action = "Would delete" if dry_run else "Deleted"
    print(f"Mode: {mode_label}")
    print(f"Files processed: {total_files}")
    print(f"{action}: {total_deleted:,} total rows")
    if failed_files:
        print(f"Failed files: {len(failed_files)}")
        for name, error in failed_files:
            print(f"  - {name}: {error}")
    else:
        print(f"Failed files: 0")

    hours = int(overall_elapsed // 3600)
    minutes = int((overall_elapsed % 3600) // 60)
    seconds = int(overall_elapsed % 60)
    if hours > 0:
        print(f"Total time: {hours}h {minutes}m {seconds}s")
    else:
        print(f"Total time: {minutes}m {seconds}s")


def delete_from_static_data(static_db_path: Path, persona_segment_ids: Set[int],
                            persona_user_ids: Set[int], dry_run: bool = False,
                            vacuum: bool = False):
    """Delete persona segments and users from staticData.sqlite3.

    This should be run AFTER all daily files have been processed, since the
    daily file processing depends on looking up segment IDs in staticData.

    Args:
        static_db_path: Path to staticData.sqlite3
        persona_segment_ids: Set of segment short_ids to delete
        persona_user_ids: Set of user short_ids to delete
        dry_run: If True, count without deleting
        vacuum: If True, VACUUM after deletion
    """
    mode_label = "DRY RUN" if dry_run else "LIVE"
    print(f"\nCleaning staticData.sqlite3 ({mode_label})")
    print("-" * 60)

    start_time = time.time()

    conn = sqlite3.connect(static_db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")
        conn.execute("PRAGMA temp_store = MEMORY")

        # Load persona segment IDs into temp table
        conn.execute("CREATE TEMP TABLE persona_seg_ids (id INTEGER PRIMARY KEY)")
        conn.executemany("INSERT INTO persona_seg_ids VALUES (?)",
                         [(sid,) for sid in persona_segment_ids])

        # Load persona user IDs into temp table
        conn.execute("CREATE TEMP TABLE persona_usr_ids (id INTEGER PRIMARY KEY)")
        conn.executemany("INSERT INTO persona_usr_ids VALUES (?)",
                         [(uid,) for uid in persona_user_ids])

        if dry_run:
            seg_count = conn.execute("""
                SELECT COUNT(*) FROM segments
                WHERE short_id IN (SELECT id FROM persona_seg_ids)
            """).fetchone()[0]
            usr_count = conn.execute("""
                SELECT COUNT(*) FROM users
                WHERE short_id IN (SELECT id FROM persona_usr_ids)
            """).fetchone()[0]
            print(f"  Would delete {seg_count:,} segments and {usr_count:,} users")
        else:
            # Delete segments first (they reference users)
            conn.execute("""
                DELETE FROM segments
                WHERE short_id IN (SELECT id FROM persona_seg_ids)
            """)
            seg_deleted = conn.execute("SELECT changes()").fetchone()[0]

            # Delete users
            conn.execute("""
                DELETE FROM users
                WHERE short_id IN (SELECT id FROM persona_usr_ids)
            """)
            usr_deleted = conn.execute("SELECT changes()").fetchone()[0]

            conn.commit()
            print(f"  Deleted {seg_deleted:,} segments and {usr_deleted:,} users")

            if vacuum:
                print(f"  Running VACUUM (this may take a while on an 8GB file)...")
                conn.execute("VACUUM")

        elapsed = time.time() - start_time
        print(f"  staticData cleanup completed ({elapsed:.1f}s)")

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Delete Persona spammer segments from SponsorBlock daily databases"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count deletions without actually deleting"
    )
    parser.add_argument(
        "--start-from", type=str, metavar="DATE",
        help="Resume from a specific date (YYYY-MM-DD), inclusive"
    )
    parser.add_argument(
        "--vacuum", action="store_true",
        help="Run VACUUM after each deletion to reclaim disk space (slow)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 60)
    print("SponsorBlock Persona Segment Deleter")
    print("=" * 60)
    print(f"Static DB:    {STATIC_DB_PATH}")
    print(f"Daily files:  {DAILY_FILES_DIR}")
    print(f"Persona file: {PERSONA_FILE}")
    if args.dry_run:
        print("Mode:         DRY RUN (no changes will be made)")
    print()

    # Phase 1: Pre-compute persona user and segment IDs from staticData
    persona_hashes = load_persona_hashes(PERSONA_FILE)
    persona_user_ids, persona_segment_ids = find_persona_ids(STATIC_DB_PATH, persona_hashes)

    if not persona_segment_ids:
        print("No persona segments found in staticData. Nothing to do.")
        return

    # Phase 2: Process daily files
    daily_files = get_daily_files(DAILY_FILES_DIR, args.start_from)

    if daily_files:
        print(f"Found {len(daily_files)} daily files to process")
        process_daily_files(daily_files, persona_segment_ids,
                            dry_run=args.dry_run, vacuum=args.vacuum)

    # Phase 3: Delete from staticData (last, since earlier phases depend on it)
    delete_from_static_data(STATIC_DB_PATH, persona_segment_ids, persona_user_ids,
                            dry_run=args.dry_run, vacuum=args.vacuum)


if __name__ == "__main__":
    main()
