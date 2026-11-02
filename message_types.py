from typing import TypedDict, List, Union, Literal

# Source Message Types
class SourceHello(TypedDict):
    type: Literal["SOURCE_HELLO"]
    session: str

class CaptionUpdate(TypedDict):
    type: Literal["CAPTION_UPDATE"]
    session: str
    segment: int
    revision: int
    source_ts: float
    final: bool
    text: str

class SourceDone(TypedDict):
    type: Literal["SOURCE_DONE"]


# Clients Message Types
class ClientHello(TypedDict):
    type: Literal["CLIENT_HELLO"]
    client_id: int
    session: str
    last_segment: int
    last_revision: int
    history_seconds: int

class History(TypedDict):
    type: Literal["HISTORY"]
    items: List[CaptionUpdate]


IncomingMessage = Union[SourceHello, CaptionUpdate, SourceDone, ClientHello]
