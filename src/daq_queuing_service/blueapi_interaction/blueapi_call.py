from blueapi.service.model import TaskRequest
from pydantic import BaseModel


class BlueapiCall(BaseModel):
    task_request: TaskRequest
    parent_task_id: str
