from log_parser import LogParser

SAMPLE_LOGS = """
2024-01-24 10:15:32.123 INFO Request processed in 127ms
2024-01-24 10:15:33.001 ERROR Database connection failed
2024-01-24 10:15:34.042 INFO Request processed in 95ms
"""

# Write the logs to a temporary file for testing
with open("sample_logs.txt", "w") as file:
    file.write(SAMPLE_LOGS)

# Test parsing the log file
log_entries = LogParser.parse_file("sample_logs.txt")
for entry in log_entries:
    print(f"Timestamp: {entry.timestamp}, Level: {entry.level}, Message: {entry.message}, Metrics: {entry.metrics}")
