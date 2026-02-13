import os
import json
import random
import sqlite3
import mimetypes
import requests
import threading

from time        import sleep, localtime, time
from bottle      import route, template, default_app, request, static_file, request, error, HTTPResponse, response
from config      import data_path, home_folder, server_folder, auth_token, test_private_ID, beta_folder, pi_file_server_url, pi_auth_token
from datetime    import datetime
from contextlib  import suppress

db_path = os.path.join(data_path, "userdata.sqlite3")
downvotes_path = os.path.join(data_path, "faileddownvotes.sqlite3")
VIPs_filepath = os.path.join(data_path, "vipUsers.csv")
usernames_path = os.path.join(data_path, "userNames.csv")

august_2020_db_path = os.path.join(data_path, "2020-08-31_sponsorTimes_mini.sqlite3")

usernames = dict() # dict of {userID: username}. updated daily
last_usernames_update = 0

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS userdata (
    userID TEXT,
    date TEXT,
    json_data TEXT,
    PRIMARY KEY (userID, date)
)
''')

# Create an index on the userID column
cursor.execute("CREATE INDEX IF NOT EXISTS idx_userID ON userdata(userID);")

with open(VIPs_filepath, "r") as f:
	VIPs = set(f.read().split("\n"))
	with suppress(KeyError):
		VIPs.remove("userID")

server_log_path = os.path.join(server_folder, "server_log.txt")

def update_vips():
	"""Periodically update the global VIPs set by reading from the VIP users file."""
	global VIPs
	while True:
		with open(VIPs_filepath, "r") as f:
			new_vips = set(f.read().split("\n"))
			with suppress(KeyError):
				new_vips.remove("userID")

		# Update the global VIPs set
		if new_vips != VIPs:
			VIPs = new_vips
			log("VIP users updated.")

		# Wait for 12 hours (43200 seconds) before updating again
		sleep(43200)

def update_usernames():
	global last_usernames_update
	# This is safe to call frequently and will only update once per day
	if (time() - last_usernames_update) > 86400:
		with open(usernames_path, "r") as f:
			rows = f.read().split("\n")
			for row in rows:
				with suppress(IndexError):
					values = row.split(",")
					userID = values[0]
					locked = values[-1]
					username = ",".join(values[1:-1])
					usernames[userID] = username
		last_usernames_update = time()

threading.Thread(target=update_vips, daemon=True).start()

def log(log_text):
	"""
	Takes a string, s, and logs it to a log file on disk with a timestamp. Also prints the string to console.
	"""
	current_time = localtime()
	year   = str(current_time.tm_year)
	month  = str(current_time.tm_mon ).zfill(2)
	day    = str(current_time.tm_mday).zfill(2)
	hour   = str(current_time.tm_hour).zfill(2)
	minute = str(current_time.tm_min ).zfill(2)
	second = str(current_time.tm_sec ).zfill(2)

	log_time = f"{year}-{month}-{day} {hour}:{minute}:{second}"
	log_text = log_text.replace("\n", "").replace("\r", "") # makes sure each log line is only one line

	print(f"{log_time} - {log_text}")
	with open(server_log_path, "a", encoding="utf-8") as f:
		f.write(log_time + " - " + log_text + "\n")

@route("/favicon.ico")
def serve_favicon():
	return static_file("LogoSponsorBlockSimple256px.png", root=home_folder)

@route("/script.js")
def serve_script():
	script_filename = "script.js"
	return static_file(script_filename, root=server_folder)

@route("/")
def leaderboard():
	page_path = os.path.join(server_folder, "leaderboard_page.html")

	last_updated=int(get_last_updated())
	last_updated = datetime.fromtimestamp(last_updated).strftime("%d/%m/%y %H:%M")
	return template(page_path, last_updated=last_updated)

# millie's new combined CSS file
@route("/leaderboardStyles.css")
def serve_leaderboard_styles():
	return static_file("leaderboardStyles.css", root=server_folder)

@route("/beta")
def leaderboard_beta():
	page_path = os.path.join(server_folder, "beta/leaderboard_page.html")

	last_updated=int(get_last_updated())
	last_updated = datetime.fromtimestamp(last_updated).strftime("%d/%m/%y %H:%M")
	return template(page_path, last_updated=last_updated)

@route("/beta/script.js")
def serve_script_beta():
	return static_file("script.js", root=beta_folder)

@route("/beta/leaderboardStyles.css")
def serve_style_beta():
	return static_file("leaderboardStyles.css", root=beta_folder)


@route("/last_db_update")
def get_last_updated():
	last_update_location = os.path.join(data_path, "last_db_update.txt")

	try:
		with open(last_update_location, "r") as f:
			return f.read()
	except FileNotFoundError:
		return "0"

@route("/leaderboard.json", method=['GET'])
def serve_leaderboard():
	file_date = request.query.get("file-date")

	if file_date is None:
		# no file date requested - send today's file
		leaderboard_path = os.path.join(data_path, "leaderboard.json")
	else:
		filename = file_date + "_leaderboard.json"
		leaderboard_location = os.path.join(data_path, "Leaderboard")
		leaderboard_path = os.path.join(leaderboard_location, filename)

	if not os.path.exists(leaderboard_path):
		return HTTPResponse(body="Not Found", status=404, headers=None)

	with open(leaderboard_path, "r") as f:
		leaderboard = json.load(f)

	for user in leaderboard:
		user["vip"] = user["ID"] in VIPs

	response.content_type = 'application/json'
	return json.dumps(leaderboard)

@route("/global_stats.json")
def serve_global_stats():
	file_date = request.query.get("file-date")

	if file_date is None:
		# no file date requested - send today's file
		globalstats_path = os.path.join(data_path, "global_stats.json")
	else:
		filename = file_date + "_global_stats.json"
		globalstats_location = os.path.join(data_path, "Global Stats")
		globalstats_path = os.path.join(globalstats_location, filename)

	if not os.path.exists(globalstats_path):
		return HTTPResponse(body="Not Found", status=404, headers=None)

	with open(globalstats_path, "r") as f:
		globalstats = json.load(f)

	response.content_type = 'application/json'
	response.headers['Access-Control-Allow-Origin'] = '*'
	response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
	response.headers['Access-Control-Allow-Headers'] = 'Origin, Content-Type, Accept'
	return json.dumps(globalstats)

def get_dates_from_filenames(directory, suffix):
	"""Helper function to extract dates from filenames in a given directory with a specific suffix."""
	files = os.listdir(directory)
	dates = set()
	for file in files:
		if file.endswith(suffix):
			date_str = file.split('_')[0]
			dates.add(date_str)
	return dates

@route("/available_dates.json")
def serve_available_dates():
	global_stats_path = os.path.join(data_path, "Global Stats")
	leaderboard_path = os.path.join(data_path, "Leaderboard")

	# Get dates from filenames
	global_stats_dates = get_dates_from_filenames(global_stats_path, "_global_stats.json")
	leaderboard_dates = get_dates_from_filenames(leaderboard_path, "_leaderboard.json")

	# Find the intersection of dates
	available_dates = sorted(global_stats_dates & leaderboard_dates)

	# Serve the available dates as JSON
	response.content_type = 'application/json'
	return json.dumps(available_dates)

@route("/leaderboardStyleLight.css")
def css_light():
	return static_file("leaderboardStyleLight.css", root=server_folder)

@route("/leaderboardStyleDark.css")
def css_dark():
	return static_file("leaderboardStyleDark.css", root=server_folder)

@route("/leaderboardStylePink.css")
def css_pink():
	return static_file("leaderboardStylePink.css", root=server_folder)

@route("/addUserData", method="POST")
def add_user_data():
	if "Authorisation" not in request.headers:
		log("Request to add user data denied: no authorisation provided")
		sleep(random.random())
		return HTTPResponse(status=403, body="Not Authorised")

	if request.headers["Authorisation"] != auth_token:
		log("Request to add user data denied: unauthorised")
		sleep(random.random())
		return HTTPResponse(status=401, body="Not Authorised")

	try:
		user_data = json.loads(request.json)
	except Exception as ex:
		log(f"Request to add user data denied: Malformed json data. Exception is: {ex}")
		return HTTPResponse(status=400, body=f"Malformed json data.")

	try:
		userID = request.query["userID"]
	except Exception as ex:
		log(f"Request to add user data denied: No userID provided. Exception is: {ex}")
		return HTTPResponse(status=400, body=f"No userID provided.")

	if len(userID) != 64:
		log(f"Request to add user data denied: incorrect length on userID {userID}")
		return HTTPResponse(status=400, body=f"UserID should be 64 characters.")

	try:
		int(userID, 16)
	except:
		log(f"Request to add user data denied: malformed userID (should be hex, was {userID}")
		return HTTPResponse(status=400, body=f"UserID should be hexadecimal.")


	validated_data = dict()

	for datestamp in user_data:
		try:
			year, month, day = datestamp.split("-")
			assert len(day)==2
			assert len(month)==2
			assert len(year)==4
		except ValueError:
			log(f"Failed to add data: malformed JSON keys - {datestamp}")
			return HTTPResponse(status=400, body=f"Malformed date {datestamp} - date should be in the format YYYY-MM-DD")
		except AssertionError:
			log(f"Failed to add data: incorrect date format - {datestamp}")
			return HTTPResponse(status=400, body=f"Malformed date {datestamp}")

		validated_data[datestamp] = user_data[datestamp]

	for datestamp in validated_data:
		json_string = json.dumps(validated_data[datestamp]) # dump json dict back to string for storage
		cursor.execute('''
			INSERT INTO userdata
			VALUES (?, ?, ?)
			ON CONFLICT(userID, date) DO UPDATE SET
				json_data = excluded.json_data
    		''', (userID, datestamp, json_string))
		conn.commit()

	return HTTPResponse(status=200, body="ok")

@route("/checkUserData", method="GET")
def check_user_data():
	if "Authorisation" not in request.headers:
		sleep(random.random())
		return HTTPResponse(status=403, body="Not Authorised")

	if request.headers["Authorisation"] != auth_token:
		sleep(random.random())
		return HTTPResponse(status=401, body="Not Authorised")

	cursor.execute("SELECT userID,date FROM userdata")
	json_data = dict()

	for row in cursor.fetchall():
		userID, date = row
		if userID in json_data:
			json_data[userID].append(date)
		else:
			json_data[userID] = [date]

	response.content_type = 'application/json'
	return json.dumps(json_data)

@route("/getUserIDs", method="GET")
def get_userIDs():
	userIDs_filepath = os.path.join(data_path, "userData_IDs.txt")
	if "Authorisation" not in request.headers:
		sleep(random.random())
		return HTTPResponse(status=403, body="Not Authorised")

	if request.headers["Authorisation"] != auth_token:
		sleep(random.random())
		return HTTPResponse(status=401, body="Not Authorised")

	with open(userIDs_filepath, "r") as f:
		userIDs = f.read().split("\n")

	response.content_type = 'application/json'
	return json.dumps(userIDs)


@route("/userdata", method="GET")
def userdata_page():
	# Set CORS headers
	response.headers['Access-Control-Allow-Origin'] = '*'
	response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
	response.headers['Access-Control-Allow-Headers'] = 'Origin, Content-Type, Accept'

	try:
		userID = request.query["userID"]
	except:
		response.status = 400
		return {"error": "No userID provided."}

	if len(userID) != 64:
		response.status = 400
		return {"error": "UserID must be 64 characters."}

	try:
		int(userID, 16)
	except ValueError:
		response.status = 400
		return {"error": "UserID must be hexadecimal."}

	cursor.execute("SELECT date,json_data FROM userdata WHERE userID=?", (userID,))
	raw_data_list = cursor.fetchall()

	if not raw_data_list:
		addUser(userID)
		response.status = 404
		return {"error": "Data for this userID is now being generated - this happens overnight so please check back tomorrow."}

	response_data = dict()

	for key, data in raw_data_list:
		response_data[key] = json.loads(data)

	# Set the content type to JSON for success responses
	response.content_type = 'application/json'
	return response_data

#wrapper of above to not break millie's code - above is deprecated and /api/ should be used going forwards
@route("/api/userdata", method="GET")
def user_data_api():
	return userdata_page()

# Call this func if a userID page is loaded which I have no data for
def addUser(new_userID):
	assert len(new_userID) == 64, "userID length should be 64"

	userIDs_filepath = os.path.join(data_path, "userData_IDs.txt")

	with open(userIDs_filepath, "r") as f:
		userIDs = f.read().split("\n")

	if new_userID not in userIDs:
		with open(userIDs_filepath, "a") as f:
			f.write("\n" + new_userID)

@route("/sponsorTimes_mini_schema.txt", method="GET")
def get_mini_schema():
	return static_file("sponsorTimes_mini_schema.txt", root=server_folder)


@route("/api/SB_api_test")
def api_test():
    # Set CORS headers
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Content-Type, Accept'

    # API endpoint
    api_url = f"https://api.sponsor.ajay.app/api/getViewsForUser?userID={test_private_ID}"

    start_time = time()

    try:
        # Set a 5 second timeout
        api_response = requests.get(api_url, timeout=5)

    except Exception:
        # For any request failure (timeout, connection error, etc), return generic message

        time_taken = int((time() - start_time) * 1000)

        return {
            "success": False,
            "code": None,
            "time_taken_ms": int((time() - start_time) * 1000),
            "error": "No response received from SponsorBlock API"
        }
    else: # no exception raised by the request
        time_taken = int((time() - start_time) * 1000)
        response_data = {
            "success": api_response.status_code == 200,
            "code": api_response.status_code,
            "time_taken_ms": time_taken
        }

        if api_response.status_code != 200:
            response_data["error"] = api_response.text

        return response_data

@route("/add_downvote", method="POST")
def add_downvote():
	try:
		token = request.headers.get("Authorisation", "")
	except Exception as ex:
		log(f"Error parsing token in add_downvote: {str(ex)}")
		return HTTPResponse(status=404, body="Not Found.")

	if token=="":
		return HTTPResponse(status=401, body="Unauthorised.")
	elif token != auth_token:
		log("Request to add downvote denied: unauthorised")
		sleep(random.random())
		return HTTPResponse(status=403, body="Not Authorised")

	try:
		request_data = request.json
	except Exception as ex:
		log(f"couldn't parse request data - {ex} - Data is: {request.json}")
		return HTTPResponse(status=400, body="Failed to parse json data.")

	if not request_data:
		return HTTPResponse(status=400, body="Missing JSON data.")

	try:
		timestamp_unix = request_data["timestamp_unix"]
		videoID        = request_data["videoID"]
		submissionID   = request_data["submissionID"]
		category       = request_data["category"]
		userID         = request_data["userID"]
	except AttributeError:
		return HTTPResponse(status=401, body="Error parsing request data.")

	with sqlite3.connect(downvotes_path) as conn:
		cur = conn.cursor()

		# Check if entry already exists
		cur.execute("""SELECT 1 FROM downvotes WHERE submissionID=? AND timestamp=?""", (submissionID, timestamp_unix))

		if not cur.fetchone():  # If no results, insert the new entry
			cur.execute("INSERT INTO downvotes VALUES(?,?,?,?,?)", (timestamp_unix, videoID, submissionID, category, userID))
			return HTTPResponse(status=201, body="Accepted")
		else:
			return HTTPResponse(status=409, body="Data already exists.")

	return HTTPResponse(status=500, body="End of function - this should never happen.")


@route("/api/downvotes")
def get_downvotes():
	limit        = request.query.get("limit", 100)
	userID       = request.query.get("userID", None)
	videoID      = request.query.get("videoID", None)
	category     = request.query.get("category", None)
	time_after   = request.query.get("timeAfter", None)
	time_before  = request.query.get("timeBefore", None)
	submissionID = request.query.get("submissionID",None)

	# Define query parameters and defaults
	query_params = {
		"userID": userID,
		"videoID": videoID,
		"time_after": time_after,
		"time_before": time_before,
		"category": category,
		"submissionID": submissionID
	}

	# Ensure timestamps are within valid range
	MIN_TIMESTAMP = 1609459200  # 1st Jan 2021
	MAX_TIMESTAMP = int(time())

	# Prepare conditions and values for SQL
	conditions = []
	values = []

	for param, value in query_params.items():
		if value:
			if param in ["time_after", "time_before"]:
				clamped_value = max(MIN_TIMESTAMP, min(int(value), MAX_TIMESTAMP))
				operator = ">=" if param == "time_after" else "<="
				conditions.append(f"timestamp {operator} ?")
				values.append(clamped_value)
			else:
				conditions.append(f"{param} = ?")
				values.append(value)

	# Set a limit with a max cap at 1000
	limit = min(int(limit), 1000)
	conditions_query = " AND ".join(conditions)
	query = f"SELECT * FROM downvotes WHERE {conditions_query} LIMIT ?"
	values.append(limit)

	# Ensure at least one condition
	if not conditions:
		response.status = 400
		return {"error": "At least one query parameter is required"}

	try:
		with sqlite3.connect(downvotes_path) as conn:
			conn.row_factory = sqlite3.Row
			cur = conn.cursor()
			cur.execute(query, values)
			results = cur.fetchall()

		response.content_type = 'application/json'
		return json.dumps([dict(row) for row in results])

	except sqlite3.Error as e:
		response.status = 500
		return {"error": str(e)}


@route("/api")
def downvotes_doc():
	return static_file("api_doc.html", root=server_folder)

@route("/status")
def serve_sb_status():
    return static_file("sb_status.html", root=server_folder)






@route('/faileddownvotes')
def serve_failed_downvote_page():
	return static_file('failed_downvotes.html', root=server_folder)

@route("/faileddownvotesbeta")
def serve_failed_downvote_beta_page():
	return static_file("failed_downvotes_beta.html", root=server_folder)

@route('/api/downvotes_between.json')
def downvotes_by_period():
	# Parse and validate parameters
	before_str = request.query.get('before')
	after_str = request.query.get('after')
	if not before_str or not after_str:
		response.status = 400
		return {"error": "Missing 'before' or 'after' parameters."}

	try:
		before_date = datetime.strptime(before_str, '%Y-%m-%d')
		after_date = datetime.strptime(after_str, '%Y-%m-%d')
		before_timestamp = int(before_date.timestamp()) + 86400 #add one day. This was if the before and after dates are the same, it searches that whole day rather than searching nothing.
		after_timestamp = int(after_date.timestamp())
	except ValueError:
		response.status = 400
		return {"error": "Invalid date format. Use 'yyyy-mm-dd'."}

	# Query the database
	conn = sqlite3.connect(downvotes_path)
	cur = conn.cursor()
	cur.execute(
		"""
		SELECT userID, submissionID, category
		FROM downvotes
		WHERE timestamp >= ? AND timestamp < ?
		""",
		(after_timestamp, before_timestamp)
	)
	downvotes = cur.fetchall()
	conn.close()

	update_usernames() # get latest usernames

	# Aggregate results by userID
	users = {}
	for userID, submissionID, category in downvotes:
		if userID == "0000000000000000000000000000000000000000000000000000000000000000":
			continue
		if userID not in users:
			users[userID] = {
				"userID": userID,
				"display_name": usernames.get(userID, userID),
				"total_downvotes": 0,
				"most_downvoted": None,
				"most_downvoted_count": 0,
				"category": None,
				"vip": userID in VIPs
			}

		# Update total downvotes
		users[userID]["total_downvotes"] += 1

		# Track most downvoted submission
		submission_counts = users[userID].setdefault("submission_counts", {})
		submission_counts[submissionID] = submission_counts.get(submissionID, 0) + 1
		if submission_counts[submissionID] > users[userID]["most_downvoted_count"]:
			users[userID]["most_downvoted"] = submissionID
			users[userID]["most_downvoted_count"] = submission_counts[submissionID]
			users[userID]["category"] = category

	# Prepare final output
	result = []
	for user_data in users.values():
		user_data.pop("submission_counts")  # Remove intermediate data
		result.append(user_data)

	# Sort by most_downvoted_count and take the top 200
	result.sort(key=lambda x: x["total_downvotes"], reverse=True)
	result = result[:200]

	# Return as JSON
	response.content_type = 'application/json'
	return json.dumps(result)

@route('/api/usernames.json')
def get_usernames():
	update_usernames()
	# Return as JSON
	response.content_type = 'application/json'
	return json.dumps(usernames)

@route('/api/downvote_leaderboard_users.json')
def failed_downvotes_users():
	return static_file('downvote_leaderboard_users.json', root=data_path)

@route('/api/downvote_leaderboard_segments.json')
def failed_downvotes_segments():
	return static_file('downvote_leaderboard_segments.json', root=data_path)


@route("/SBcoin")
def serve_SBCoin():
	coin_db = "/home/AcesFullOfKings/SBCoin/SBCoin_ledger.db"

	conn = sqlite3.connect(coin_db)
	cursor = conn.cursor()
	cursor.execute("Select receiver_id,amount from transactions")
	transactions = cursor.fetchall()

	cursor.execute("Select userID, username from users")
	user_results = cursor.fetchall()

	users = {userID: username for userID, username in user_results}

	user_coins = dict()
	for row in transactions:
		userID,amount = row
		username = users.get(userID, userID)
		if username in user_coins:
			user_coins[username] += amount
		else:
			user_coins[username] = amount

	page_path = os.path.join(server_folder, "SBCoin_leaderboard.html")
	return template(page_path, coin_data=user_coins, title="SBCoin Leaderboard", coin_column="SBCoin Owned")

@route("/SBcoinGifters")
def serve_SBCoin_gifters():
	coin_db = "/home/AcesFullOfKings/SBCoin/SBCoin_ledger.db"

	conn = sqlite3.connect(coin_db)
	cursor = conn.cursor()
	cursor.execute("Select awarder_id,receiver_id,amount from transactions")
	transactions = cursor.fetchall()

	cursor.execute("Select userID, username from users")
	user_results = cursor.fetchall()

	users = {userID: username for userID, username in user_results}

	user_coins = dict()
	for row in transactions:
		awarder,receiver,amount = row
		if awarder != receiver and amount > 0 and awarder != "gamble":
			username = users.get(awarder, awarder)
			if username in user_coins:
				user_coins[username] += amount
			else:
				user_coins[username] = amount

	page_path = os.path.join(server_folder, "SBCoin_leaderboard.html")
	return template(page_path, coin_data=user_coins, title="SBCoin Gifters Leaderboard", coin_column="SBCoin Gifted")

@route("/SBcoinGamblers")
def serve_SBCoin_gamblers():
	coin_db = "/home/AcesFullOfKings/SBCoin/SBCoin_ledger.db"

	conn = sqlite3.connect(coin_db)
	cursor = conn.cursor()
	cursor.execute("Select awarder_id,receiver_id,amount from transactions")
	transactions = cursor.fetchall()

	cursor.execute("Select userID, username from users")
	user_results = cursor.fetchall()

	users = {userID: username for userID, username in user_results}

	user_coins = dict()
	for row in transactions:
		awarder,receiver,amount = row
		if awarder == "gamble" or awarder==receiver:
			username = users.get(receiver, receiver)
			if username in user_coins:
				user_coins[username] += amount
			else:
				user_coins[username] = amount

	page_path = os.path.join(server_folder, "SBCoin_leaderboard.html")
	return template(page_path, coin_data=user_coins, title="SBCoin Gamblers Leaderboard", coin_column="Net Winnings")

@route('/SBCoin_ledger.db', method='GET')
def serve_ledger():
	log(f"Request received for SBCoin_ledger.db from IP: {request.remote_addr}")
	return static_file('SBCoin_ledger.db', root='/home/AcesFullOfKings/SBCoin', download=True)


# Rate limiting storage - add this at the top of your file with other globals
ip_request_times = {}  # Dict to store request times per IP

def check_rate_limits(ip_address):
    """Check if IP or global rate limits are exceeded. Returns True if rate limited."""
    current_time = time()
    cutoff_time = current_time - 60

    # Clean old requests from all IPs
    for ip in list(ip_request_times.keys()):
        ip_request_times[ip] = [t for t in ip_request_times[ip] if t > cutoff_time]
        if not ip_request_times[ip]:
            del ip_request_times[ip]

    # Check IP rate limit
    ip_request_count = len(ip_request_times.get(ip_address, []))
    if ip_request_count >= 6:
        return True

    # Check global rate limit by counting all recent requests
    total_requests = sum(len(times) for times in ip_request_times.values())
    if total_requests >= 30:
        return True

    # Record this request
    if ip_address not in ip_request_times:
        ip_request_times[ip_address] = []
    ip_request_times[ip_address].append(current_time)

    return False

@route("/true_votes", method="GET")
def true_votes():
    try:
        client_ip = request.remote_addr

        # Check rate limits
        if check_rate_limits(client_ip):
            log(f"Rate limit exceeded for IP {client_ip}")
            response.content_type = 'application/json'
            http_response = {"error": "Too many requests. Please try again later."}
            return HTTPResponse(body=json.dumps(http_response), status=429, headers=None)

        segment_id = request.query.get("segment_ID")

        if not segment_id:
            response.content_type = 'application/json'
            http_response = {"error": "No segment ID provided. segment_ID parameter is required."}
            log(f"True Votes request received from {client_ip} - No segment ID provided")
            return HTTPResponse(body=json.dumps(http_response), status=400, headers=None)

        api_result = requests.get(f"https://sponsor.ajay.app/api/segmentInfo?UUID={segment_id}")

        if api_result.status_code in [400,404]:
            response.content_type = 'application/json'
            http_response = {"error": "Segment does not exist."}
            log(f"True Votes request received from {client_ip} - {segment_id}: Not Found (status {api_result.status_code})")
            return HTTPResponse(body=json.dumps(http_response), status=404, headers=None)

        api_json = api_result.json()
        current_votes = api_json[0].get("votes",0)

        conn = sqlite3.connect(august_2020_db_path)
        cursor = conn.cursor()
        cursor.execute("select votes from sponsorTimes where UUID=?", (segment_id,))
        db_result = cursor.fetchone()

        if not db_result:
            old_votes = 0
        else:
            old_votes = db_result[0]

        # if current_votes = -2:
        # ?? what do? it was removed so prob current_votes < old_votes.

        http_response = {
            "true_votes": current_votes - old_votes,
            "current_votes": current_votes,
            "ignored_votes": old_votes
        }

        log(f"True Votes request received from {client_ip} - {segment_id}: {http_response}")

        response.content_type = 'application/json'
        return json.dumps(http_response)

    except Exception as ex:
        log(f"Exception: {ex}")
        return HTTPResponse(body="Server error.", status=503, headers=None)


## Archive download proxy routes
## These forward requests to the Raspberry Pi file server, hiding its IP from end users.

@route("/archive")
def serve_archive_page():
	return static_file("archive.html", root=server_folder)

@route("/archive/files.json")
def archive_files_json():
	try:
		pi_response = requests.get(
			f"{pi_file_server_url}/api/files",
			headers={"Authorization": pi_auth_token},
			timeout=10
		)
	except requests.exceptions.ConnectionError:
		return HTTPResponse(status=502, body='{"error": "Archive server is currently offline."}')
	except requests.exceptions.Timeout:
		return HTTPResponse(status=504, body='{"error": "Archive server timed out."}')
	except requests.exceptions.RequestException as e:
		log(f"Archive proxy error (file list): {e}")
		return HTTPResponse(status=502, body='{"error": "Could not reach archive server."}')

	if pi_response.status_code != 200:
		return HTTPResponse(status=pi_response.status_code, body=pi_response.text)

	response.content_type = "application/json"
	return pi_response.text

@route("/archive/download/<filename>")
def archive_download(filename):
	log(f"Archive download request: {filename} from {request.remote_addr}")

	try:
		pi_response = requests.get(
			f"{pi_file_server_url}/api/download",
			params={"file": filename},
			headers={"Authorization": pi_auth_token},
			stream=True,
			timeout=300
		)
	except requests.exceptions.ConnectionError:
		return HTTPResponse(status=502, body="Archive server is currently offline. Please try again later.")
	except requests.exceptions.Timeout:
		return HTTPResponse(status=504, body="Download timed out. Please try again later.")
	except requests.exceptions.RequestException as e:
		log(f"Archive proxy error (download {filename}): {e}")
		return HTTPResponse(status=502, body="Could not reach archive server.")

	if pi_response.status_code != 200:
		return HTTPResponse(status=pi_response.status_code, body=pi_response.text)

	response.content_type = "application/octet-stream"
	content_disp = pi_response.headers.get("Content-Disposition", f'attachment; filename="{filename}"')
	response.headers["Content-Disposition"] = content_disp
	content_length = pi_response.headers.get("Content-Length")
	if content_length:
		response.headers["Content-Length"] = content_length

	def stream():
		for chunk in pi_response.iter_content(chunk_size=65536):
			yield chunk

	return stream()


## Public files directory listing
## Serves any files dropped into the public folder on the NAS.

@route("/files")
def serve_files_page():
	return static_file("files.html", root=server_folder)

@route("/files/list.json")
def files_list_json():
	try:
		pi_response = requests.get(
			f"{pi_file_server_url}/api/public_files",
			headers={"Authorization": pi_auth_token},
			timeout=10
		)
	except requests.exceptions.ConnectionError:
		return HTTPResponse(status=502, body='{"error": "File server is currently offline."}')
	except requests.exceptions.Timeout:
		return HTTPResponse(status=504, body='{"error": "File server timed out."}')
	except requests.exceptions.RequestException as e:
		log(f"Files proxy error (file list): {e}")
		return HTTPResponse(status=502, body='{"error": "Could not reach file server."}')

	if pi_response.status_code != 200:
		return HTTPResponse(status=pi_response.status_code, body=pi_response.text)

	response.content_type = "application/json"
	return pi_response.text

@route("/files/download/<filename>")
def files_download(filename):
	log(f"Public file download request: {filename} from {request.remote_addr}")

	try:
		pi_response = requests.get(
			f"{pi_file_server_url}/api/download",
			params={"file": filename},
			headers={"Authorization": pi_auth_token},
			stream=True,
			timeout=300
		)
	except requests.exceptions.ConnectionError:
		return HTTPResponse(status=502, body="File server is currently offline. Please try again later.")
	except requests.exceptions.Timeout:
		return HTTPResponse(status=504, body="Download timed out. Please try again later.")
	except requests.exceptions.RequestException as e:
		log(f"Files proxy error (download {filename}): {e}")
		return HTTPResponse(status=502, body="Could not reach file server.")

	if pi_response.status_code != 200:
		return HTTPResponse(status=pi_response.status_code, body=pi_response.text)

	response.content_type = "application/octet-stream"
	content_disp = pi_response.headers.get("Content-Disposition", f'attachment; filename="{filename}"')
	response.headers["Content-Disposition"] = content_disp
	content_length = pi_response.headers.get("Content-Length")
	if content_length:
		response.headers["Content-Length"] = content_length

	def stream():
		for chunk in pi_response.iter_content(chunk_size=65536):
			yield chunk

	return stream()

@route("/files/view/<filename>")
def files_view(filename):
	log(f"Public file view request: {filename} from {request.remote_addr}")

	try:
		pi_response = requests.get(
			f"{pi_file_server_url}/api/download",
			params={"file": filename, "inline": "1"},
			headers={"Authorization": pi_auth_token},
			stream=True,
			timeout=300
		)
	except requests.exceptions.ConnectionError:
		return HTTPResponse(status=502, body="File server is currently offline. Please try again later.")
	except requests.exceptions.Timeout:
		return HTTPResponse(status=504, body="Download timed out. Please try again later.")
	except requests.exceptions.RequestException as e:
		log(f"Files proxy error (view {filename}): {e}")
		return HTTPResponse(status=502, body="Could not reach file server.")

	if pi_response.status_code != 200:
		return HTTPResponse(status=pi_response.status_code, body=pi_response.text)

	content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
	response.content_type = content_type
	response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
	content_length = pi_response.headers.get("Content-Length")
	if content_length:
		response.headers["Content-Length"] = content_length

	def stream():
		for chunk in pi_response.iter_content(chunk_size=65536):
			yield chunk

	return stream()


application = default_app()

if __name__ == "__main__":
	application.run(host="localhost", port=8080)#, debug=True, reloader=True)