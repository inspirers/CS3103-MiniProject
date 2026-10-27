from typing import Any

# Type alias for message from source or clients
type Message = dict[str, Any]

class DataQueue:
    """Queue structure designed specifically to handle data received from source.
       - Data must be parse with json.loads first
       - Handles revision deletion via tombstones (None).
       - General rule of thumb, rebuild when tombstones > capacity / 2
       - Likely bugs: when updating indices, ensure to modulo with capacity

    Attributes:
        capacity: Maximum amount of data that the queue can hold
        queue: Underlying list that contains the data
        segment_indices: Indices within 'queue' of the segments stored (for efficient deletion)
        front: Index of the first element (for pop)
        rear: Index of the next element to be pushed
        true_size: True size of queue including tombstones
        tombstones: Number of tombstones present in the queue

    Outward Methods:
        push(data) -> bool: Push data into the queue. Returns true if successful
        pop() -> data: pop data from the queue (removes tombstones until data is found)
        delete_past_revisions(segment): Delete all revisions of that segment 
            except the latest revision / final revision
        delete_all_past_revisions(): Deletes all revisions except the latest revision / final revision
        size() -> int: returns the number of elements in the queue (excluding tombstones) 
        is_full() -> bool: True if the queue physically cannot accomodate any more data 
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.queue: list[Message | None]= [None] * self.capacity
        self.segment_indices: dict[int, list[int]] = {}
        self.front = 0
        self.rear = 0
        self.true_size = 0
        self.tombstones = 0

    def push(self, data: Message) -> bool:
        # Queue is truly full
        if self.is_full():
            return False
        # Queue is full but has more space because of tombstones
        if self.is_capacity_reached():
            self.rebuild()

        self.queue[self.rear] = data
        self.segment_indices.setdefault(data["segment"], []).append(self.rear)
        self.rear = (self.rear + 1) % self.capacity
        self.true_size = self.true_size + 1
        return True

    def pop(self) -> Message | None:
        # Clearing tombstones
        while self.queue[self.front] is None:
            self.front = (self.front + 1) % self.capacity
            self.tombstones -= 1
            self.true_size -= 1
            if self.true_size == 0:
                return None

        data = self.queue[self.front]
        self.segment_indices[data["segment"]].remove(self.front)
        if len(self.segment_indices[data["segment"]]) == 0:
            self.segment_indices.pop(data["segment"])
        self.front = (self.front + 1) % self.capacity
        self.true_size -= 1
        return data

    def delete_past_revisions(self, segment: int) -> None:
        segment_indices = self.segment_indices.get(segment)
        if segment_indices is None or len(segment_indices) == 0:
            return
        most_updated = self.get_most_updated(segment)
        for idx in segment_indices:
            if idx != most_updated:
                self.queue[idx] = None # Set as tombstone
                self.tombstones += 1
        self.segment_indices[segment] = [most_updated]
        return

    def delete_all_past_revisions(self) -> None:
        for segment in self.segment_indices:
            self.delete_past_revisions(segment)
        if self.tombstones > self.capacity / 2:
            self.rebuild()
        return
    
    def rebuild(self) -> None:
        new_queue = [None] * self.capacity
        new_indices = {}
        index = 0
        for data in self.queue:
            if data is None:
                continue
            new_queue[index] = data
            new_indices.setdefault(data["segment"], []).append(index)
            index += 1
        self.queue = new_queue
        self.segment_indices = new_indices
        self.front = 0
        self.rear = index
        self.true_size = index
        self.tombstones = 0

    def get_most_updated(self, segment: int) -> int | None:
        segment_indices = self.segment_indices.get(segment)
        if segment_indices is None or len(segment_indices) == 0:
            return None
        best = segment_indices[0]
        for idx in segment_indices:
            if self.queue[idx]["final"]:
                return idx
            if self.queue[idx]["revision"] > self.queue[best]["revision"]:
                best = idx
        return best

    def size(self) -> int:
        return self.true_size - self.tombstones
    
    def is_full(self) -> bool:
        return self.true_size - self.tombstones == self.capacity
    
    def is_capacity_reached(self) -> bool:
        return self.true_size == self.capacity
    
    