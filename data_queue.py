from __future__ import annotations

from typing import Any, Dict, Optional

# Type alias for message from source or clients
Message = Dict[str, Any]

class SourceQueue:
    def __init__(self, window: int = 90) -> None:
        self.history: dict[int, Message] = {}
        self.window = window
        self.latest_timestamp: int = 0
        self.earliest_timestamp: Optional[int] = None

    def add(self, data: Message) -> bool:
        segment = data["segment"]
        source_ts = data["source_ts"]
        if segment in self.history:
            if (self.history[segment]["final"] 
                or self.history[segment]["revision"] >= data["revision"]
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

    def get_latest(self, window: int = 60, segment: int = 0, revision: int = 0) -> list[Message]:
        result: list[Message] = []
        highest_seg = max(self.history.keys()) if self.history else None

        for seg, data in self.history.items():
            lifetime = self.latest_timestamp - data["source_ts"]
            is_within_window = lifetime <= window
            is_newer_segment = seg > segment
            is_newer_revision_of_same = (seg == segment and data.get("revision", 0) > revision)
            is_active_open = (seg == highest_seg and not data.get("final", False)) # Keep the highest non-final segment

            if (data.get("final", False) or is_active_open) and is_within_window and (is_newer_segment or is_newer_revision_of_same):
                result.append(data)

        return sorted(result, key=lambda c: (c["source_ts"], c["segment"]))

    def reset(self) -> None:
        self.history.clear()
        self.latest_timestamp = 0
        self.earliest_timestamp = None

    def cleanup(self) -> None:
        to_delete: list[int] = []
        highest_seg = max(self.history.keys()) if self.history else None

        for seg, data in self.history.items():
            lifetime = self.latest_timestamp - data["source_ts"]
            if lifetime > self.window:
                # Preserve the highest non-final segment's highest revision
                if seg == highest_seg and not data.get("final", False):
                    continue
                to_delete.append(seg)

        for seg in to_delete:
            del self.history[seg]

        if self.history:
            self.earliest_timestamp = min(data["source_ts"] for data in self.history.values())
        else:
            self.earliest_timestamp = self.latest_timestamp