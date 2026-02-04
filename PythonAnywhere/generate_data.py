import os
import json
import matplotlib.pyplot as plt
from datetime import datetime

# Directory containing the JSON files
data_dir = "data/Global Stats"

# Metrics you want to extract and plot
metrics_to_plot = [
	"contributing_users",
	"overall_submissions",
	"overall_time_saved",
	"overall_skips"
]

# Dictionary to store {metric: {date: value}}
metric_data = {metric: {} for metric in metrics_to_plot}

# Loop through files in the data directory
print("Processing JSON files...")
for filename in sorted(os.listdir(data_dir)):
	if filename.endswith("_global_stats.json"):
		date_str = filename.split("_")[0]
		print(f"Reading: {filename}")
		try:
			with open(os.path.join(data_dir, filename), 'r') as f:
				data = json.load(f)
				date_obj = datetime.strptime(date_str, "%Y-%m-%d")
				for metric in metrics_to_plot:
					value = data.get(metric, None)
					if value is not None:
						metric_data[metric][date_obj] = value
		except Exception as e:
			print(f"  Error reading {filename}: {e}")

# Plot each metric
print("\nGenerating plots...")
for metric, data in metric_data.items():
	if not data:
		print(f"  Skipping empty metric: {metric}")
		continue

	sorted_dates = sorted(data.keys())
	sorted_values = [data[date] for date in sorted_dates]

	plt.figure(figsize=(12, 6))
	plt.plot(sorted_dates, sorted_values, marker='o', linestyle='-')
	plt.title(f"{metric.replace('_', ' ').title()} Over Time")
	plt.xlabel("Date")
	plt.ylabel(metric.replace('_', ' ').title())
	plt.grid(True)
	plt.tight_layout()

	# Save the plot
	output_filename = f"{metric}.png"
	plt.savefig(output_filename)
	plt.close()
	print(f"  Saved plot: {output_filename}")

print("\nDone.")
