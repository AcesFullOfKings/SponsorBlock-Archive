import sqlite3
import json
import os
import csv

from contextlib import suppress

db_path = "/home/AcesFullOfKings/server/data/faileddownvotes.sqlite3"
leaderboard_output_path_users = "/home/AcesFullOfKings/server/data/downvote_leaderboard_users.json"
leaderboard_output_path_segments = "/home/AcesFullOfKings/server/data/downvote_leaderboard_segments.json"
VIPs_filepath = "/home/AcesFullOfKings/server/data/vipUsers.csv"
usernames_filepath = "/home/AcesFullOfKings/server/data/userNames.csv"

with open(VIPs_filepath, "r") as f:
	VIPs = set(f.read().split("\n"))
	with suppress(KeyError):
		VIPs.remove("userID")

usernames = dict()

# Read the CSV file
with open(usernames_filepath, "r") as csvfile:
	reader = csv.reader(csvfile)
	# Skip the header row if it exists
	next(reader, None)
	for row in reader:
		user_id, username, _ = row  # Unpack the row, ignoring the 'locked' field
		usernames[user_id] = username

conn = sqlite3.connect(db_path)
cur = conn.cursor()

user_stats    = dict()
segment_stats = dict()

for userID, segmentID, category in cur.execute("""SELECT userID, submissionID, category FROM downvotes"""):
	if userID != "0000000000000000000000000000000000000000000000000000000000000000":
		if segmentID not in segment_stats:
			segment_stats[segmentID] = dict()
			segment_stats[segmentID]["votes"] = 0
			segment_stats[segmentID]["userID"] = userID
			segment_stats[segmentID]["category"] = category

		segment_stats[segmentID]["votes"] += 1

		if userID not in user_stats:
			user_stats[userID] = {'total_downvotes': 0, 'segment_counts': dict()}

		if segmentID not in user_stats[userID]['segment_counts']:
			user_stats[userID]['segment_counts'][segmentID] = [category, 0]

		user_stats[userID]['segment_counts'][segmentID][1] += 1
		user_stats[userID]['total_downvotes'] += 1

#find most-downvoted segs
for userID in user_stats:
	most_downvoted = ""
	most_downvotes = 0
	most_downvoted_category = ""
	for segmentID in user_stats[userID]['segment_counts']:
		category, votes = user_stats[userID]['segment_counts'][segmentID]
		if votes > most_downvotes:
			most_downvoted = segmentID
			most_downvoted_category = category
			most_downvotes = votes

	user_stats[userID]["most_downvoted"] = most_downvoted
	user_stats[userID]["most_downvoted_count"] = most_downvotes
	user_stats[userID]["most_downvoted_category"] = most_downvoted_category

	del user_stats[userID]['segment_counts']

# Prepare the leaderboard data
leaderboard = []
segs_leaderboard = []

for userID, stats in user_stats.items():
	leaderboard.append({
		"userID"               : userID,
		"display_name"         : usernames[userID] if userID in usernames else userID,
		"total_downvotes"      : stats["total_downvotes"],
		"most_downvoted"       : stats["most_downvoted"],
		"most_downvoted_count" : stats["most_downvoted_count"],
		"category"             : stats["most_downvoted_category"],
		"vip"                  : userID in VIPs
	})


for segmentID, segment_stats in segment_stats.items():
	userID = segment_stats["userID"]
	segs_leaderboard.append({
		"segmentID"            : segmentID,
		"userID"               : userID,
		"display_name"         : usernames[userID] if userID in usernames else userID,
		"votes"                : segment_stats["votes"],
		"category"             : segment_stats["category"],
		"vip"                  : userID in VIPs
	})


# Sort by total downvotes
leaderboard.sort(key=lambda x: x['total_downvotes'], reverse=True)
leaderboard = leaderboard [:200]

# Sort by total downvotes
segs_leaderboard.sort(key=lambda x: x['votes'], reverse=True)
segs_leaderboard = segs_leaderboard[:200]

# Write users json
with open(leaderboard_output_path_users, 'w') as f:
	json.dump(leaderboard, f)

# Write segments json
with open(leaderboard_output_path_segments, 'w') as f:
	json.dump(segs_leaderboard, f)

# Close the database connection
conn.close()
