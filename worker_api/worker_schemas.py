from enum import Enum

from pydantic import BaseModel, Field
from datetime import datetime, time

from typing import List, Optional, Union

class WorkerCallBack(BaseModel):
    location_id: int
    terminal_id: int
    session_id: int
    status: str
    msg: str

