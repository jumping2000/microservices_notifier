from pydantic import BaseModel, ConfigDict


class ChannelRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    enabled: bool


class ChannelUpdate(BaseModel):
    enabled: bool
