"""
Helper functions for querying SponsorBlock archive databases.

This module provides utility functions to look up segment and user information
from the processed SQLite databases.
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
import re


# =============================================================================
# Configuration - Update these paths for your environment
# =============================================================================

# Path to the static data database containing segment metadata
STATIC_DB_PATH = "./archive/staticData.sqlite3"

# Directory containing daily segment databases (YYYY-MM-DD_segmentData.sqlite3)
DAILY_DB_DIR = "./archive/Daily Files"


# =============================================================================
# Helper Functions
# =============================================================================

def get_available_dates() -> List[str]:
    """
    Get a sorted list of all available dates from the daily database files.

    Returns:
        List of date strings in YYYY-MM-DD format, sorted chronologically.
    """
    daily_dir = Path(DAILY_DB_DIR)

    if not daily_dir.exists():
        raise FileNotFoundError(f"Daily database directory not found: {DAILY_DB_DIR}")

    # Pattern for daily database files: YYYY-MM-DD_segmentData.sqlite3
    pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})_segmentData\.sqlite3$')
    dates = []

    for file_path in daily_dir.iterdir():
        if file_path.is_file():
            match = pattern.match(file_path.name)
            if match:
                dates.append(match.group(1))

    return sorted(dates)


def get_latest_date() -> str:
    """
    Get the most recent available date.

    Returns:
        Date string in YYYY-MM-DD format.
    """
    dates = get_available_dates()
    if not dates:
        raise RuntimeError(f"No daily database files found in {DAILY_DB_DIR}")
    return dates[-1]


def get_segment_info(segment_uuid: str, date: Optional[str] = None) -> Optional[Dict]:
    """
    Get all information about a segment for a specific date.

    Args:
        segment_uuid: The segment's long UUID (64 characters).
        date: Date in YYYY-MM-DD format. If None, uses the latest available date.

    Returns:
        Dictionary containing segment information, or None if not found.
        Keys: uuid, video_id, user_id (long UUID), start_time, end_time, category,
              action_type, time_submitted, hidden, shadow_hidden, locked, votes, views, date
    """
    if date is None:
        date = get_latest_date()

    static_db_path = Path(STATIC_DB_PATH)
    if not static_db_path.exists():
        raise FileNotFoundError(f"Static database not found: {STATIC_DB_PATH}")

    daily_db_path = Path(DAILY_DB_DIR) / f"{date}_segmentData.sqlite3"
    if not daily_db_path.exists():
        raise FileNotFoundError(f"Daily database not found for date {date}: {daily_db_path}")

    # Connect to static database and get segment metadata
    static_conn = sqlite3.connect(static_db_path)
    static_conn.row_factory = sqlite3.Row
    static_cursor = static_conn.cursor()

    # Look up segment by UUID and join with user to get user's long_id
    static_cursor.execute("""
        SELECT
            s.short_id, s.long_id as uuid, s.video_id,
            u.long_id as user_id,
            s.start_time, s.end_time, s.category, s.action_type,
            s.time_submitted, s.hidden, s.shadow_hidden, s.locked
        FROM segments s
        JOIN users u ON s.user_id = u.short_id
        WHERE s.long_id = ?
    """, (segment_uuid,))

    row = static_cursor.fetchone()

    if row is None:
        static_conn.close()
        return None

    # Extract static data
    segment_short_id = row['short_id']
    segment_info = {
        'uuid': row['uuid'],
        'video_id': row['video_id'],
        'user_id': row['user_id'],
        'start_time': row['start_time'],
        'end_time': row['end_time'],
        'category': row['category'],
        'action_type': row['action_type'],
        'time_submitted': row['time_submitted'],
        'hidden': bool(row['hidden']),
        'shadow_hidden': bool(row['shadow_hidden']),
        'locked': bool(row['locked']),
        'date': date
    }

    static_conn.close()

    # Connect to daily database and get votes/views
    daily_conn = sqlite3.connect(daily_db_path)
    daily_conn.row_factory = sqlite3.Row
    daily_cursor = daily_conn.cursor()

    daily_cursor.execute("""
        SELECT votes, views
        FROM segment_data
        WHERE segment_id = ?
    """, (segment_short_id,))

    daily_row = daily_cursor.fetchone()
    daily_conn.close()

    if daily_row:
        segment_info['votes'] = daily_row['votes']
        segment_info['views'] = daily_row['views']
    else:
        # Segment exists in static DB but not in this day's data (shouldn't happen normally)
        segment_info['votes'] = None
        segment_info['views'] = None

    return segment_info


def get_user_segments(user_uuid: str, date: Optional[str] = None) -> List[Dict]:
    """
    Get all segments submitted by a user for a specific date.

    Args:
        user_uuid: The user's long UUID (64 characters).
        date: Date in YYYY-MM-DD format. If None, uses the latest available date.

    Returns:
        List of dictionaries, each containing segment information in the same format
        as get_segment_info(). Returns empty list if user not found.
    """
    if date is None:
        date = get_latest_date()

    static_db_path = Path(STATIC_DB_PATH)
    if not static_db_path.exists():
        raise FileNotFoundError(f"Static database not found: {STATIC_DB_PATH}")

    daily_db_path = Path(DAILY_DB_DIR) / f"{date}_segmentData.sqlite3"
    if not daily_db_path.exists():
        raise FileNotFoundError(f"Daily database not found for date {date}: {daily_db_path}")

    # Connect to static database
    static_conn = sqlite3.connect(static_db_path)
    static_conn.row_factory = sqlite3.Row
    static_cursor = static_conn.cursor()

    # Look up user's short_id
    static_cursor.execute("""
        SELECT short_id FROM users WHERE long_id = ?
    """, (user_uuid,))

    user_row = static_cursor.fetchone()

    if user_row is None:
        static_conn.close()
        return []

    user_short_id = user_row['short_id']

    # Get all segments by this user
    static_cursor.execute("""
        SELECT
            short_id, long_id as uuid, video_id,
            start_time, end_time, category, action_type,
            time_submitted, hidden, shadow_hidden, locked
        FROM segments
        WHERE user_id = ?
        ORDER BY time_submitted
    """, (user_short_id,))

    segments = []
    segment_id_map = {}  # Map short_id to index in segments list

    for row in static_cursor.fetchall():
        segment_info = {
            'uuid': row['uuid'],
            'video_id': row['video_id'],
            'user_id': user_uuid,
            'start_time': row['start_time'],
            'end_time': row['end_time'],
            'category': row['category'],
            'action_type': row['action_type'],
            'time_submitted': row['time_submitted'],
            'hidden': bool(row['hidden']),
            'shadow_hidden': bool(row['shadow_hidden']),
            'locked': bool(row['locked']),
            'date': date,
            'votes': None,
            'views': None
        }
        segment_id_map[row['short_id']] = len(segments)
        segments.append(segment_info)

    static_conn.close()

    # Connect to daily database and get votes/views for all segments
    daily_conn = sqlite3.connect(daily_db_path)
    daily_conn.row_factory = sqlite3.Row
    daily_cursor = daily_conn.cursor()

    # Get all votes/views for these segments in one query
    segment_ids = tuple(segment_id_map.keys())

    if segment_ids:
        # Use WHERE IN for efficient bulk lookup
        placeholders = ','.join('?' * len(segment_ids))
        daily_cursor.execute(f"""
            SELECT segment_id, votes, views
            FROM segment_data
            WHERE segment_id IN ({placeholders})
        """, segment_ids)

        # Update segments with votes/views data
        for row in daily_cursor.fetchall():
            segment_idx = segment_id_map[row['segment_id']]
            segments[segment_idx]['votes'] = row['votes']
            segments[segment_idx]['views'] = row['views']

    daily_conn.close()

    return segments


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Example: Get all available dates
    print("Available dates:")
    dates = get_available_dates()
    print(f"  First: {dates[0] if dates else 'None'}")
    print(f"  Latest: {dates[-1] if dates else 'None'}")
    print(f"  Total: {len(dates)} dates")
    print()

    # Example: Get segment info
    # Replace with an actual UUID from your database
    example_uuid = "0" * 64  # Placeholder
    print(f"Looking up segment {example_uuid[:16]}...")
    segment = get_segment_info(example_uuid)
    if segment:
        print(f"  Found: video {segment['video_id']}, {segment['votes']} votes")
    else:
        print("  Not found")
    print()

    # Example: Get user segments
    # Replace with an actual user UUID from your database
    example_user = "0" * 64  # Placeholder
    print(f"Looking up segments by user {example_user[:16]}...")
    user_segments = get_user_segments(example_user)
    print(f"  Found {len(user_segments)} segments")
    if user_segments:
        print(f"  First segment: video {user_segments[0]['video_id']}")
