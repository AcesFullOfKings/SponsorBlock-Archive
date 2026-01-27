#!/usr/bin/env python3
"""
SponsorBlock Archive Converter

Converts sponsorTimes.csv files into compressed SQLite databases:
- staticData.sqlite3: Contains user mappings and segment metadata (grows over time)
- YYYY-MM-DD_segmentData.sqlite3: Contains daily votes and views data

Usage:
    python convert_csv_to_sqlite.py <path_to_csv_file>

Example:
    python convert_csv_to_sqlite.py 2026-01-15_sponsorTimes.csv
"""

import sqlite3
import sys
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Dict, Tuple
import config

def create_static_db_schema(conn: sqlite3.Connection):
    """Create the schema for staticData.sqlite3 if it doesn't exist."""
    cursor = conn.cursor()

    # Users table: maps long UUIDs to short integer IDs
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            short_id INTEGER PRIMARY KEY AUTOINCREMENT,
            long_id TEXT UNIQUE NOT NULL
        )
    """)

    # Segments table: contains all static segment metadata
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS segments (
            short_id INTEGER PRIMARY KEY AUTOINCREMENT,
            long_id TEXT UNIQUE NOT NULL,
            video_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            start_time REAL NOT NULL,
            end_time REAL NOT NULL,
            category TEXT NOT NULL,
            action_type TEXT NOT NULL,
            time_submitted INTEGER NOT NULL,
            hidden BOOLEAN NOT NULL,
            shadow_hidden BOOLEAN NOT NULL,
            locked BOOLEAN NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(short_id)
        )
    """)

    conn.commit()


def create_daily_db_schema(conn: sqlite3.Connection):
    """Create the schema for daily segment data databases."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS segment_data (
            segment_id INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            views INTEGER NOT NULL
        )
    """)

    conn.commit()


def get_or_create_user_id(cursor: sqlite3.Cursor, user_long_id: str,
                          user_cache: Dict[str, int]) -> int:
    """Get the short_id for a user, creating a new entry if needed."""
    # Check cache first
    if user_long_id in user_cache:
        return user_cache[user_long_id]

    # Check database
    cursor.execute("SELECT short_id FROM users WHERE long_id = ?", (user_long_id,))
    result = cursor.fetchone()

    if result:
        user_cache[user_long_id] = result[0]
        return result[0]

    # Create new user
    cursor.execute("INSERT INTO users (long_id) VALUES (?)", (user_long_id,))
    short_id = cursor.lastrowid
    user_cache[user_long_id] = short_id
    return short_id


def get_or_create_segment_id(cursor: sqlite3.Cursor, segment_uuid: str,
                             segment_data: Dict[str, any],
                             segment_cache: Dict[str, int]) -> int:
    """
    Get the short_id for a segment, creating or updating the entry as needed.

    If the segment exists, update hidden/shadow_hidden/locked fields.
    If the segment is new, create it with all metadata.
    """
    # Check cache first
    if segment_uuid in segment_cache:
        # Update the segment's status fields if it already exists
        cursor.execute("""
            UPDATE segments
            SET hidden = ?, shadow_hidden = ?, locked = ?
            WHERE short_id = ?
        """, (
            segment_data['hidden'],
            segment_data['shadow_hidden'],
            segment_data['locked'],
            segment_cache[segment_uuid]
        ))
        return segment_cache[segment_uuid]

    # Check database
    cursor.execute("SELECT short_id FROM segments WHERE long_id = ?", (segment_uuid,))
    result = cursor.fetchone()

    if result:
        # Update status fields
        cursor.execute("""
            UPDATE segments
            SET hidden = ?, shadow_hidden = ?, locked = ?
            WHERE short_id = ?
        """, (
            segment_data['hidden'],
            segment_data['shadow_hidden'],
            segment_data['locked'],
            result[0]
        ))
        segment_cache[segment_uuid] = result[0]
        return result[0]

    # Create new segment
    cursor.execute("""
        INSERT INTO segments (
            long_id, video_id, user_id, start_time, end_time,
            category, action_type, time_submitted,
            hidden, shadow_hidden, locked
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        segment_uuid,
        segment_data['video_id'],
        segment_data['user_id'],
        segment_data['start_time'],
        segment_data['end_time'],
        segment_data['category'],
        segment_data['action_type'],
        segment_data['time_submitted'],
        segment_data['hidden'],
        segment_data['shadow_hidden'],
        segment_data['locked']
    ))

    short_id = cursor.lastrowid
    segment_cache[segment_uuid] = short_id
    return short_id


def extract_date_from_filename(filename: str) -> str:
    """Extract the date (YYYY-MM-DD) from a CSV filename."""
    match = re.match(r'(\d{4}-\d{2}-\d{2})_', filename)
    if not match:
        raise ValueError(f"Could not extract date from filename: {filename}")
    return match.group(1)


def import_csv_to_temp_db(csv_path: Path) -> Path:
    """Import CSV file into a temporary SQLite database using native SQLite import."""
    temp_db_path = csv_path.parent / f"temp_import_{csv_path.stem}.db"

    # Remove temp database if it exists from a previous run
    if temp_db_path.exists():
        temp_db_path.unlink()

    print(f"Importing CSV into temporary database using SQLite...")

    # Use SQLite command line to import CSV
    # This is much faster than reading CSV in Python
    cmd = f'sqlite3 "{temp_db_path}" ".mode csv" ".import {csv_path.absolute()} sponsorTimes"'

    result = os.system(cmd)

    if result != 0:
        raise RuntimeError(f"Failed to import CSV file using SQLite")

    print(f"CSV imported successfully to temporary database")
    return temp_db_path


def process_csv_file(csv_path: str):
    """Main function to process a sponsorTimes.csv file."""
    process_start_time = time.time() # careful - the variable "start_time" (a string) is already used below ;)

    csv_path = Path(csv_path)

    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)

    # Extract date from filename
    date_str = extract_date_from_filename(csv_path.name)
    print(f"Processing CSV for date: {date_str}")

    # Set up database paths
    static_db_path = Path(config.STATIC_DB_PATH)
    daily_db_dir = Path(config.DAILY_DB_DIR)
    daily_db_dir.mkdir(parents=True, exist_ok=True)
    daily_db_path = daily_db_dir / f"{date_str}_segmentData.sqlite3"

    print(f"Static database: {static_db_path}")
    print(f"Daily database: {daily_db_path}")

    # Import CSV to temporary database
    temp_db_path = import_csv_to_temp_db(csv_path)

    try:
        # Connect to databases
        temp_conn = sqlite3.connect(temp_db_path)
        static_conn = sqlite3.connect(static_db_path)
        daily_conn = sqlite3.connect(daily_db_path)

        # Optimize SQLite settings for bulk inserts
        print("Optimizing database settings for bulk insert...")
        for conn in [static_conn, daily_conn]:
            conn.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging for better concurrency
            conn.execute("PRAGMA synchronous = NORMAL")  # Faster commits, still safe
            conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            conn.execute("PRAGMA temp_store = MEMORY")  # Keep temp tables in memory

        # Create schemas WITHOUT indexes (we'll create them after bulk insert)
        create_static_db_schema(static_conn)
        create_daily_db_schema(daily_conn)

        static_conn.commit()
        daily_conn.commit()

        # Prepare cursors
        temp_cursor = temp_conn.cursor()
        static_cursor = static_conn.cursor()
        daily_cursor = daily_conn.cursor()

        # Caches to avoid repeated database lookups
        user_cache: Dict[str, int] = {}
        segment_cache: Dict[str, int] = {}

        # Start transactions for better performance
        static_conn.execute("BEGIN TRANSACTION")
        daily_conn.execute("BEGIN TRANSACTION")

        # Get total row count
        temp_cursor.execute("SELECT COUNT(*) FROM sponsorTimes")
        total_rows = temp_cursor.fetchone()[0]
        print(f"Processing {total_rows:,} rows...")

        # Process rows from temporary database
        row_count = 0
        temp_cursor.execute("SELECT * FROM sponsorTimes")

        for row in temp_cursor:
            row_count += 1

            if row_count % 100000 == 0:
                print(f"Committing batch at {row_count:,} rows...")
                static_conn.commit()
                daily_conn.commit()
                static_conn.execute("BEGIN TRANSACTION")
                daily_conn.execute("BEGIN TRANSACTION")

            # Parse row (columns in CSV order)
            # Use _ for unused columns that we're dropping
            video_id, start_time, end_time, votes, locked, _, uuid, \
                user_id_long, time_submitted, views, category, action_type, _, \
                _, hidden, _, shadow_hidden, _, \
                _, _ = row

            # Get or create user ID
            user_short_id = get_or_create_user_id(
                static_cursor,
                user_id_long,
                user_cache
            )

            # Prepare segment data
            segment_data = {
                'video_id': video_id,
                'user_id': user_short_id,
                'start_time': float(start_time),
                'end_time': float(end_time),
                'category': category,
                'action_type': action_type,
                'time_submitted': int(float(time_submitted)),
                'hidden': int(hidden),
                'shadow_hidden': int(shadow_hidden),
                'locked': int(locked)
            }

            # Get or create segment ID
            segment_short_id = get_or_create_segment_id(
                static_cursor,
                uuid,
                segment_data,
                segment_cache
            )

            # Insert daily data
            daily_cursor.execute("""
                INSERT INTO segment_data (segment_id, votes, views)
                VALUES (?, ?, ?)
            """, (
                segment_short_id,
                int(votes),
                int(views)
            ))

        print(f"Finished processing {row_count:,} rows")

        # Commit final transaction
        print("Committing final changes to databases...")
        static_conn.commit()
        daily_conn.commit()

        # Create indexes now that bulk insert is complete
        print("Creating indexes (this may take a few minutes)...")

        print("  Creating index on users.long_id...")
        static_conn.execute("CREATE INDEX IF NOT EXISTS idx_users_long_id ON users(long_id)")

        print("  Creating index on segments.long_id...")
        static_conn.execute("CREATE INDEX IF NOT EXISTS idx_segments_long_id ON segments(long_id)")

        print("  Creating index on segments.video_id...")
        static_conn.execute("CREATE INDEX IF NOT EXISTS idx_segments_video_id ON segments(video_id)")

        print("  Creating index on segment_data.segment_id...")
        daily_conn.execute("CREATE INDEX IF NOT EXISTS idx_segment_data_id ON segment_data(segment_id)")

        static_conn.commit()
        daily_conn.commit()
        print("Indexes created successfully")

        # Optimize database files
        print("Optimizing database files...")
        static_conn.execute("VACUUM")
        daily_conn.execute("VACUUM")
        print("Database optimization complete")

        # Close connections
        temp_conn.close()
        static_conn.close()
        daily_conn.close()

        print("Conversion complete!")
        print(f"Static database updated: {static_db_path}")
        print(f"Daily database created: {daily_db_path}")

        # Show file sizes
        static_size_mb = static_db_path.stat().st_size / (1024 * 1024)
        daily_size_mb = daily_db_path.stat().st_size / (1024 * 1024)
        csv_size_mb = csv_path.stat().st_size / (1024 * 1024)

        print(f"\nFile sizes:")
        print(f"  Original CSV: {csv_size_mb:.2f} MB")
        print(f"  Static DB: {static_size_mb:.2f} MB")
        print(f"  Daily DB: {daily_size_mb:.2f} MB")
        print(f"  Total: {static_size_mb + daily_size_mb:.2f} MB")
        print(f"  Compression ratio: {csv_size_mb / (static_size_mb + daily_size_mb):.2f}x")

        # Show total time taken
        process_elapsed_time = time.time() - process_start_time
        minutes = int(process_elapsed_time // 60)
        seconds = int(process_elapsed_time % 60)
        print(f"\nTime taken: {minutes}m {seconds}s")

    finally:
        # Clean up temporary database
        if temp_db_path.exists():
            print(f"Cleaning up temporary database...")
            temp_db_path.unlink()
            print("Temporary database removed")


def main():
    if len(sys.argv) != 2:
        print("Usage: python convert_csv_to_sqlite.py <path_to_csv_file>")
        print("Example: python convert_csv_to_sqlite.py 2026-01-15_sponsorTimes.csv")
        sys.exit(1)

    csv_path = sys.argv[1]
    process_csv_file(csv_path)


if __name__ == "__main__":
    main()
