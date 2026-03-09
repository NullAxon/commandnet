import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class Event(BaseModel):
    """Represents a state transition event triggering a node execution."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str
    node_name: str
    payload: Optional[Dict[str, Any]] = None
    
    # --- Scheduling & Idempotency ---
    timestamp: str = Field(default_factory=utcnow_iso)
    run_at: str = Field(default_factory=utcnow_iso) # ISO 8601 string
    idempotency_key: Optional[str] = None
