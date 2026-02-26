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

import shutil
import sqlite3
import sys
import os
import re
import time
from pathlib import Path
from typing import Dict
import config


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
	else:
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


def import_csv_to_temp_db_python_fallback(csv_path: Path) -> Path:
	"""Import CSV using Python when SQLite import fails (handles encoding errors)."""
	import csv

	temp_db_path = Path(config.TEMP_DIR) / f"temp_import_{csv_path.stem}.db"

	# Remove temp database if it exists from a previous run
	if temp_db_path.exists():
		temp_db_path.unlink()

	print(f"Importing CSV using Python fallback (with encoding error handling)...")

	# Create database and table
	conn = sqlite3.connect(temp_db_path)
	cursor = conn.cursor()

	# Create table matching CSV structure
	cursor.execute("""
		CREATE TABLE sponsorTimes (
			videoID TEXT, startTime TEXT, endTime TEXT, votes TEXT, locked TEXT,
			incorrectVotes TEXT, UUID TEXT, userID TEXT, timeSubmitted TEXT,
			views TEXT, category TEXT, actionType TEXT, service TEXT,
			videoDuration TEXT, hidden TEXT, reputation TEXT, shadowHidden TEXT,
			hashedVideoID TEXT, userAgent TEXT, description TEXT
		)
	""")

	# Read CSV with error handling
	skipped_rows = 0
	imported_rows = 0

	with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
		reader = csv.reader(f)
		next(reader)  # Skip header

		for row_num, row in enumerate(reader, start=2):
			try:
				if len(row) != 20:
					skipped_rows += 1
					continue

				cursor.execute("""
					INSERT INTO sponsorTimes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
				""", row)
				imported_rows += 1

				# Commit every 100k rows
				if imported_rows % 100000 == 0:
					conn.commit()
					print(f"  Imported {imported_rows:,} rows...")

			except Exception as e:
				skipped_rows += 1
				if skipped_rows <= 5:
					print(f"  Warning: Skipping row {row_num} due to error: {e}")

	conn.commit()
	conn.close()

	if skipped_rows > 0:
		print(f"CSV imported with {skipped_rows:,} rows skipped due to errors")
	else:
		print(f"CSV imported successfully")

	return temp_db_path


def import_csv_to_temp_db(csv_path: Path) -> Path:
	"""Import CSV file into a temporary SQLite database using native SQLite import."""
	temp_db_path = Path(config.TEMP_DIR) / f"temp_import_{csv_path.stem}.db"

	# Remove temp database if it exists from a previous run
	if temp_db_path.exists():
		temp_db_path.unlink()

	print(f"Importing CSV into temporary database using SQLite...")

	# Use SQLite command line to import CSV
	# This is much faster than reading CSV in Python
	cmd = f'sqlite3 "{temp_db_path}" ".mode csv" ".import {csv_path.absolute()} sponsorTimes"'

	result = os.system(cmd)

	if result != 0:
		# SQLite import failed, try Python fallback
		print(f"SQLite import failed (possibly due to encoding errors), using Python fallback...")
		return import_csv_to_temp_db_python_fallback(csv_path)

	print(f"CSV imported successfully to temporary database")
	return temp_db_path


def process_csv_file(csv_path: str):
	"""Main function to process a sponsorTimes.csv file."""
	process_start_time = time.time() # careful - the variable "start_time" (a string) is already used below ;)

	# Create temp directory if it doesn't exist
	temp_dir = Path(config.TEMP_DIR)
	temp_dir.mkdir(parents=True, exist_ok=True)

	csv_path = Path(csv_path)

	if not csv_path.exists():
		print(f"Error: CSV file not found: {csv_path}")
		sys.exit(1)

	# Extract date from filename
	date_str = extract_date_from_filename(csv_path.name)
	print(f"Processing CSV for date: {date_str}")

	# Set up database paths
	static_db_path_local = Path(config.STATIC_DB_PATH_LOCAL)
	static_db_path_whitebox = Path(config.STATIC_DB_PATH_WHITEBOX)

	daily_db_dir = Path(config.DAILY_DB_DIR)
	daily_db_final_path = daily_db_dir / f"{date_str}_segmentData.sqlite3"

	# Create daily database locally in temp directory for faster writes
	daily_db_temp_path = Path(config.TEMP_DIR) / f"{date_str}_segmentData_temp.sqlite3"

	print(f"Static database: {static_db_path_local}")
	print(f"Daily database (temp): {daily_db_temp_path}")
	print(f"Daily database (final): {daily_db_final_path}")

	# Import CSV to temporary database
	temp_db_path = import_csv_to_temp_db(csv_path)

	try:
		# Connect to databases
		temp_conn = sqlite3.connect(temp_db_path)
		# Handle UTF-8 decode errors gracefully when reading from temp database
		temp_conn.text_factory = lambda b: b.decode(errors='replace')

		static_conn = sqlite3.connect(static_db_path_local)
		# Create daily database locally for faster writes
		daily_conn = sqlite3.connect(daily_db_temp_path)

		# Optimize SQLite settings for bulk inserts
		print("Optimizing database settings for bulk insert...")
		for conn in [static_conn, daily_conn]:
			conn.execute("PRAGMA journal_mode = OFF")   # I don't need journalling: if the writes fail I can just re-run this script
			conn.execute("PRAGMA synchronous = OFF")    # This is single threaded so no need for synchronous access
			conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
			conn.execute("PRAGMA temp_store = MEMORY")  # Keep temp tables in memory

		# Prepare cursors
		temp_cursor = temp_conn.cursor()
		static_cursor = static_conn.cursor()
		daily_cursor = daily_conn.cursor()

		daily_cursor.execute("""
			CREATE TABLE IF NOT EXISTS segment_data (
				segment_id INTEGER NOT NULL,
				votes INTEGER NOT NULL,
				views INTEGER NOT NULL
			)
		""")

		daily_conn.commit()

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
		skipped_rows = 0
		temp_cursor.execute("SELECT * FROM sponsorTimes")

		for row in temp_cursor:
			row_count += 1

			if row_count % 100000 == 0:
				print(f"Committing batch at {row_count:,} rows...")
				static_conn.commit()
				daily_conn.commit()
				static_conn.execute("BEGIN TRANSACTION")
				daily_conn.execute("BEGIN TRANSACTION")

			try:
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

			except Exception as e:
				skipped_rows += 1
				# Only print first few errors to avoid spam
				if skipped_rows <= 5:
					print(f"Warning: Skipping row {row_count} due to error: {e}")
				elif skipped_rows == 6:
					print(f"Warning: More errors detected, suppressing further messages...")
				continue

		if skipped_rows > 0:
			print(f"Finished processing {row_count:,} rows ({skipped_rows:,} rows skipped due to errors)")
		else:
			print(f"Finished processing {row_count:,} rows")

		# Commit final transaction
		print("Committing final changes to databases...")
		static_conn.commit()
		daily_conn.commit()

		# Close connections
		temp_conn.close()
		static_conn.close()
		daily_conn.close()

		# Move daily database to final location
		print("Moving databases to final location...")
		
		shutil.move(str(daily_db_temp_path), str(daily_db_final_path))
		print(f"  Daily file moved to: {daily_db_final_path}")

		shutil.copy2(str(static_db_path_local), str(static_db_path_whitebox)+".tmp") # copy the static db to the NAS, but as a temp file
		os.rename(str(static_db_path_whitebox)+".tmp", str(static_db_path_whitebox)) #rename the temp file to replace the original

		print("Conversion complete!")
		print(f"Static database updated: {static_db_path_local}")
		print(f"Daily database created: {daily_db_final_path}")

		# Show file sizes
		static_size_mb = static_db_path_local.stat().st_size / (1024 * 1024)
		daily_size_mb = daily_db_final_path.stat().st_size / (1024 * 1024)
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
		# Ensure all database connections are closed to prevent locks
		print("Cleaning up...")
		try:
			if 'temp_conn' in locals() and temp_conn:
				temp_conn.close()
				print("  Closed temporary database connection")
		except:
			pass

		try:
			if 'static_conn' in locals() and static_conn:
				static_conn.close()
				print("  Closed static database connection")
		except:
			pass

		try:
			if 'daily_conn' in locals() and daily_conn:
				daily_conn.close()
				print("  Closed daily database connection")
		except:
			pass

		# Clean up temporary database files
		if temp_db_path.exists():
			temp_db_path.unlink()
			print("  Removed temporary import database")

		# Clean up temp daily database if it still exists (error case)
		if daily_db_temp_path.exists():
			daily_db_temp_path.unlink()
			print("  Removed incomplete daily database")


def main():
	if len(sys.argv) != 2:
		print("Usage: python convert_csv_to_sqlite.py <path_to_csv_file>")
		print("Example: python convert_csv_to_sqlite.py 2026-01-15_sponsorTimes.csv")
		sys.exit(1)

	csv_path = sys.argv[1]
	process_csv_file(csv_path)


if __name__ == "__main__":
	main()
