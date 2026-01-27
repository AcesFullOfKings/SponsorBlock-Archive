#!/usr/bin/env python3
"""
7z Archive Processor for SponsorBlock Data

Processes 7z archives containing monthly SponsorBlock CSV files.
Extracts one file at a time, converts to SQLite databases, then cleans up.

Usage:
    python process_7z_archive.py <path_to_7z_file> [options]

Options:
    --temp-dir <path>   Temporary directory for extraction (default: current directory)
    --force             Reprocess files even if daily database already exists

Example:
    python process_7z_archive.py sponsorTimes_2024-01.7z
    python process_7z_archive.py sponsorTimes_2024-01.7z --temp-dir /tmp
"""

import subprocess
import sys
import re
import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple
import argparse

import config
from convert_csv_to_sqlite import process_csv_file, extract_date_from_filename


def log_error(error_log_path: Path, filename: str, error_msg: str):
    """Log an error to the error log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(error_log_path, 'a') as f:
        f.write(f"[{timestamp}] FAILED: {filename} - {error_msg}\n")


def list_archive_contents(archive_path: Path) -> List[str]:
    """List all files in the 7z archive."""
    print(f"Listing contents of {archive_path.name}...")

    cmd = [config.SEVENZ_COMMAND, 'l', '-slt', str(archive_path)]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to list archive contents: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError(f"7-Zip command '{config.SEVENZ_COMMAND}' not found. "
                         f"Please install 7-Zip or update SEVENZ_COMMAND in config.py")

    # Parse output to extract filenames
    files = []
    current_file = None

    for line in result.stdout.split('\n'):
        line = line.strip()
        if line.startswith('Path = ') and not line.endswith('.7z'):
            # Extract filename (skip the archive itself)
            filename = line.split('Path = ', 1)[1]
            if filename and not filename.endswith('.7z'):
                current_file = filename
        elif line.startswith('Folder = -') and current_file:
            # This is a file (not a folder)
            files.append(current_file)
            current_file = None

    return files


def filter_csv_files(files: List[str]) -> List[str]:
    """Filter for CSV files matching the expected pattern and sort by date."""
    csv_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}_sponsorTimes\.csv$')

    csv_files = []
    skipped = []

    for filename in files:
        # Get just the filename without path
        basename = Path(filename).name

        if basename.endswith('.csv'):
            if csv_pattern.match(basename):
                csv_files.append(filename)
            else:
                skipped.append(basename)

    if skipped:
        print(f"Warning: Skipping {len(skipped)} CSV files with unexpected names:")
        for name in skipped[:5]:  # Show first 5
            print(f"  - {name}")
        if len(skipped) > 5:
            print(f"  ... and {len(skipped) - 5} more")

    # Sort by date (filename starts with YYYY-MM-DD)
    csv_files.sort()

    return csv_files


def extract_single_file(archive_path: Path, filename: str, temp_dir: Path) -> Path:
    """Extract a single file from the archive."""
    cmd = [config.SEVENZ_COMMAND, 'e', str(archive_path), filename, f'-o{temp_dir}', '-y']

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        extracted_path = temp_dir / Path(filename).name

        if not extracted_path.exists():
            raise RuntimeError(f"File was not extracted: {extracted_path}")

        return extracted_path
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to extract file: {e.stderr}")


def get_daily_db_path(date_str: str) -> Path:
    """Get the path for a daily database given a date string."""
    daily_db_dir = Path(config.DAILY_DB_DIR)
    return daily_db_dir / f"{date_str}_segmentData.sqlite3"


def process_archive(archive_path: Path, temp_dir: Path, force: bool = False):
    """Main function to process a 7z archive."""
    start_time = time.time()

    if not archive_path.exists():
        print(f"Error: Archive not found: {archive_path}")
        sys.exit(1)

    error_log_path = Path("processing_errors.log")

    print(f"Processing archive: {archive_path}")
    print(f"Temporary directory: {temp_dir}")
    print()

    # Step 1: List and filter archive contents
    all_files = list_archive_contents(archive_path)
    csv_files = filter_csv_files(all_files)

    if not csv_files:
        print("Error: No valid CSV files found in archive")
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV file(s) to process")
    print()

    # Step 2: Process each CSV file
    processed = 0
    skipped = 0
    failed = 0
    failed_files = []

    for i, csv_filename in enumerate(csv_files, 1):
        basename = Path(csv_filename).name

        try:
            # Extract date from filename
            date_str = extract_date_from_filename(basename)
            daily_db_path = get_daily_db_path(date_str)

            # Check if already processed (resume capability)
            if daily_db_path.exists() and not force:
                print(f"[{i}/{len(csv_files)}] SKIP: {basename} (already processed)")
                skipped += 1
                continue

            print(f"[{i}/{len(csv_files)}] Processing: {basename}")
            print(f"  Extracting...")

            # Extract single file
            extracted_csv = extract_single_file(archive_path, csv_filename, temp_dir)

            try:
                print(f"  Converting to SQLite...")
                # Process the CSV file
                process_csv_file(str(extracted_csv))

                # Verify the daily database was created
                if not daily_db_path.exists():
                    raise RuntimeError("Daily database was not created")

                print(f"  SUCCESS: {basename}")
                processed += 1

            except Exception as e:
                # Conversion failed - clean up incomplete database
                print(f"  FAILED: {basename} - {str(e)}")

                if daily_db_path.exists():
                    print(f"  Deleting incomplete database...")
                    daily_db_path.unlink()

                # Log the error
                log_error(error_log_path, basename, str(e))
                failed += 1
                failed_files.append(basename)

            finally:
                # Always delete the extracted CSV to save space
                if extracted_csv.exists():
                    extracted_csv.unlink()

        except Exception as e:
            # Extraction or other error
            print(f"  FAILED: {basename} - {str(e)}")
            log_error(error_log_path, basename, str(e))
            failed += 1
            failed_files.append(basename)

        print()

    # Step 3: Verify completion
    print("=" * 60)
    print("Verification")
    print("=" * 60)

    missing = []
    for csv_filename in csv_files:
        basename = Path(csv_filename).name
        try:
            date_str = extract_date_from_filename(basename)
            daily_db_path = get_daily_db_path(date_str)

            if not daily_db_path.exists():
                missing.append(basename)
        except:
            missing.append(basename)

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total files in archive: {len(csv_files)}")
    print(f"Successfully processed: {processed}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Failed: {failed}")

    if failed > 0:
        print(f"\nFailed files:")
        for filename in failed_files:
            print(f"  - {filename}")
        print(f"\nError log: {error_log_path.absolute()}")

    if missing:
        print(f"\nWarning: {len(missing)} files are missing daily databases:")
        for filename in missing[:10]:
            print(f"  - {filename}")
        if len(missing) > 10:
            print(f"  ... and {len(missing) - 10} more")
    else:
        print(f"\nAll {len(csv_files)} files have been successfully converted!")

    # Show total time
    elapsed_time = time.time() - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    if hours > 0:
        print(f"\nTotal time: {hours}h {minutes}m {seconds}s")
    else:
        print(f"\nTotal time: {minutes}m {seconds}s")


def main():
    parser = argparse.ArgumentParser(
        description='Process 7z archives containing SponsorBlock CSV files'
    )
    parser.add_argument('archive', help='Path to the 7z archive file')
    parser.add_argument('--temp-dir', default='.',
                       help='Temporary directory for extraction (default: current directory)')
    parser.add_argument('--force', action='store_true',
                       help='Reprocess files even if daily database already exists')

    args = parser.parse_args()

    archive_path = Path(args.archive)
    temp_dir = Path(args.temp_dir)

    # Create temp directory if it doesn't exist
    temp_dir.mkdir(parents=True, exist_ok=True)

    process_archive(archive_path, temp_dir, args.force)


if __name__ == "__main__":
    main()
