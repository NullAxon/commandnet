import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str
    node_name: str
    payload: Optional[Dict[str, Any]] = None
    
    timestamp: str = Field(default_factory=utcnow_iso)
    run_at: str = Field(default_factory=utcnow_iso) 
    idempotency_key: Optional[str] = None
