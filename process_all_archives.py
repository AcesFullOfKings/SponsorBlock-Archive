#!/usr/bin/env python3
"""
Archive Batch Processor for SponsorBlock Data

Processes all .7z archives from a network folder containing monthly SponsorBlock data.
Copies each archive locally, processes it, then deletes the local copy before moving
to the next file.

Usage:
    python process_all_archives.py [options]

Options:
    --temp-dir <path>   Local directory for temporary archive copies (default: current directory)
    --force             Reprocess archives even if daily databases already exist
    --start-from <file> Start processing from a specific archive file (for resuming)

Example:
    python process_all_archives.py
    python process_all_archives.py --temp-dir ./temp
    python process_all_archives.py --start-from sponsorTimes_2023-06.7z
"""

import sys
import shutil
import time
import re
from pathlib import Path
from typing import List
import argparse

import config
from process_7z_archive import process_archive


def list_network_archives(network_path: Path) -> List[Path]:
    """List all .7z archives in the network folder and sort chronologically."""
    if not network_path.exists():
        raise RuntimeError(f"Network archive path does not exist: {network_path}")

    if not network_path.is_dir():
        raise RuntimeError(f"Network archive path is not a directory: {network_path}")

    print(f"Scanning network folder: {network_path}")

    # Find all .7z files matching the sponsorTimes_YYYY-MM.7z pattern
    pattern = re.compile(r'^sponsorTimes_\d{4}-\d{2}\.7z$')
    archives = []

    for file_path in network_path.iterdir():
        if file_path.is_file() and pattern.match(file_path.name):
            archives.append(file_path)

    if not archives:
        raise RuntimeError(f"No .7z archive files found in {network_path}")

    # Sort chronologically by filename (YYYY-MM pattern sorts correctly)
    archives.sort(key=lambda p: p.name)

    print(f"Found {len(archives)} archive file(s)")
    return archives


def should_process_archive(archive_name: str, start_from: str = None) -> bool:
    """Determine if an archive should be processed based on start_from parameter."""
    if start_from is None:
        return True

    # Compare filenames alphabetically (chronological for YYYY-MM format)
    return archive_name >= start_from


def copy_archive_locally(network_path: Path, local_dir: Path) -> Path:
    """Copy archive from network to local directory."""
    local_path = local_dir / network_path.name

    # Check if local copy already exists (from failed previous run)
    if local_path.exists():
        print(f"  WARNING: Local copy already exists, removing it...")
        local_path.unlink()

    # Get file size for progress indication
    size_gb = network_path.stat().st_size / (1024 ** 3)
    print(f"  Copying from network ({size_gb:.2f} GB)...")

    copy_start = time.time()
    shutil.copy2(network_path, local_path)
    copy_time = time.time() - copy_start

    copy_speed_mbps = (size_gb * 1024) / copy_time if copy_time > 0 else 0
    print(f"  Copy complete ({copy_speed_mbps:.1f} MB/s)")

    return local_path


def process_all_archives_main(temp_dir: Path, force: bool = False, start_from: str = None):
    """Main function to process all archives from network folder."""
    overall_start_time = time.time()

    # Get network archive source path
    network_path = Path(config.NETWORK_ARCHIVE_SOURCE)

    print("=" * 80)
    print("SponsorBlock Archive Batch Processor")
    print("=" * 80)
    print(f"Network source: {network_path}")
    print(f"Local temp dir: {temp_dir}")
    print(f"Force reprocess: {force}")
    if start_from:
        print(f"Starting from: {start_from}")
    print()

    # Create temp directory if needed
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Get list of archives
    try:
        archives = list_network_archives(network_path)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Filter archives if start_from specified
    if start_from:
        archives = [a for a in archives if should_process_archive(a.name, start_from)]
        if not archives:
            print(f"ERROR: No archives found at or after '{start_from}'")
            sys.exit(1)
        print(f"Processing {len(archives)} archive(s) starting from {archives[0].name}")

    print()

    # Process each archive
    total_archives = len(archives)
    processed_archives = 0
    failed_archives = []

    for i, network_archive in enumerate(archives, 1):
        print("=" * 80)
        print(f"Archive {i}/{total_archives}: {network_archive.name}")
        print("=" * 80)
        print()

        local_archive = None

        try:
            # Step 1: Copy archive locally
            local_archive = copy_archive_locally(network_archive, temp_dir)

            # Step 2: Process the archive
            print(f"  Processing archive...")
            print()
            process_archive(local_archive, temp_dir, force)

            print()
            print(f"  Archive {network_archive.name} completed successfully")
            processed_archives += 1

        except KeyboardInterrupt:
            print()
            print("=" * 80)
            print("INTERRUPTED BY USER")
            print("=" * 80)
            print(f"Processed {processed_archives}/{total_archives} archives before interruption")
            print(f"To resume, use: --start-from {network_archive.name}")
            raise

        except Exception as e:
            print()
            print(f"  ERROR processing {network_archive.name}: {e}")
            failed_archives.append((network_archive.name, str(e)))

        finally:
            # Always clean up local copy
            if local_archive and local_archive.exists():
                print(f"  Deleting local copy...")
                try:
                    local_archive.unlink()
                    print(f"  Local copy deleted")
                except Exception as e:
                    print(f"  WARNING: Failed to delete local copy: {e}")

        print()

    # Final summary
    print()
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Total archives: {total_archives}")
    print(f"Successfully processed: {processed_archives}")
    print(f"Failed: {len(failed_archives)}")

    if failed_archives:
        print()
        print("Failed archives:")
        for name, error in failed_archives:
            print(f"  - {name}: {error}")

    # Show total time
    elapsed_time = time.time() - overall_start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    print()
    if hours > 0:
        print(f"Total time: {hours}h {minutes}m {seconds}s")
    else:
        print(f"Total time: {minutes}m {seconds}s")

    if failed_archives:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Process all .7z archives from network folder'
    )
    parser.add_argument('--temp-dir', default='.',
                       help='Local directory for temporary archive copies (default: current directory)')
    parser.add_argument('--force', action='store_true',
                       help='Reprocess archives even if daily databases already exist')
    parser.add_argument('--start-from',
                       help='Start processing from a specific archive file (for resuming)')

    args = parser.parse_args()

    temp_dir = Path(args.temp_dir)

    try:
        process_all_archives_main(temp_dir, args.force, args.start_from)
    except KeyboardInterrupt:
        sys.exit(130)  # Standard exit code for SIGINT


if __name__ == "__main__":
    main()
