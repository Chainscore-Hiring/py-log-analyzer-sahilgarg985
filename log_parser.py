from datetime import datetime
from typing import List
import re

class LogEntry:
    """Represents a structured log entry."""
    def __init__(self, timestamp: datetime, level: str, message: str, metrics: dict):
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.metrics = metrics

class LogParser:
    """Parses log files into structured LogEntry objects."""
    LOG_PATTERN = re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) "
        r"(?P<level>[A-Z]+) (?P<message>.*?)(?: in (?P<time>\d+)ms)?$"
    )

    @staticmethod
    def parse_line(line: str) -> LogEntry:
        """Parses a single log line into a LogEntry."""
        match = LogParser.LOG_PATTERN.match(line)
        if not match:
            raise ValueError(f"Invalid log format: {line.strip()}")

        timestamp = datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
        level = match.group("level")
        message = match.group("message")
        metrics = {}

        # Extract response time if available
        if match.group("time"):
            metrics["response_time"] = int(match.group("time"))

        return LogEntry(timestamp, level, message, metrics)

    @staticmethod
    def parse_file(filepath: str) -> List[LogEntry]:
        """Parses an entire log file into a list of LogEntry objects."""
        log_entries = []
        with open(filepath, "r") as file:
            for line in file:
                if line.strip():  # Skip empty lines
                    log_entries.append(LogParser.parse_line(line))
        return log_entries
