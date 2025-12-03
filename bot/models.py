from dataclasses import dataclass
from typing import Optional

@dataclass
class Subscription:
    id: int
    user_id: int
    base: str
    quote: str
    asset_type: str
    operator: str
    threshold: float
    is_active: int
    last_eval: Optional[int]