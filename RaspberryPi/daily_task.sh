LOCKFILE="/tmp/daily_task.lock"

# Check if lock file exists
if [ -f $LOCKFILE ]; then
  echo "Job is already running."
  exit 1
else
  # Create lock file
  touch $LOCKFILE

  # Make sure to delete the lock file when the script exits
  trap "rm -f $LOCKFILE" EXIT

  cd ~/Desktop
  bash mount.sh

  cd /home/james/Stuff/SponsorBlock
  /usr/bin/python3 daily_task.py

  # Job done, lock file will be deleted by trap
fi
