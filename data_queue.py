from typing import Any, Optional

# Type alias for message from source or clients
type Message = dict[str, Any]

class SourceQueue:
    def __init__(self, window = 60) -> None:
        self.history: dict[int, Message] = {}
        self.window = window
        self.latest_timestamp: int = 0
        self.earliest_timestamp: Optional[int] = None

    def add(self, data: Message) -> bool:
        segment = data["segment"]
        source_ts = data["source_ts"]
        if segment in self.history:
            if (self.history[segment]["final"] 
                or self.history[segment]["revision"] > data["revision"] 
                or self.latest_timestamp - source_ts > self.window):
                return False

        self.history[segment] = data

        self.latest_timestamp = max(self.latest_timestamp, source_ts)

        if self.earliest_timestamp is None:
            self.earliest_timestamp = source_ts
        else:
            self.earliest_timestamp = min(self.earliest_timestamp, source_ts)

        if self.latest_timestamp - self.earliest_timestamp > 2 * self.window:
            self.cleanup()
        
        return True

    def get_latest(self, window = 60, segment = 0, revision = 0) -> list[Message]:
        result: list[Message] = []
        for seg, data in self.history.items():
            lifetime = self.latest_timestamp - data["source_ts"]
            is_within_window = lifetime <= window
            is_newer_segment = seg > segment
            is_newer_revision_of_same = (seg == segment and data.get("revision", 0) > revision)
            
            if is_within_window and (is_newer_segment or is_newer_revision_of_same):
                result.append(data)

        return result

    def cleanup(self):
        to_delete: list[int] = []
        for seg, data in self.history.items():
            lifetime = self.latest_timestamp - data["source_ts"]
            if lifetime > self.window:
                to_delete.append(seg)

        for seg in to_delete:
            del self.history[seg]

        if self.history:
            self.earliest_timestamp = min(data["source_ts"] for data in self.history.values())
        else:
            self.earliest_timestamp = self.latest_timestamp
