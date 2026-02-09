from pydantic import BaseModel, Field, field_validator
import re

class KoboUpdateSchema(BaseModel) :
    
    server_url: str = Field(..., description="Kobo Server URL")
    token: str = Field(..., min_length=10)
    asset_id: str = Field(..., min_length=5)
    record_id: str | None = Field(None, alias="_id", description="The unique numeric ID of the Kobo record")

    @field_validator('server_url', mode='before')
    @classmethod
    def add_https_if_missing(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            return v
        v = v.strip().lower()
        
        if not v.startswith(('http://', 'https://')):
            v = f'https://{v}'
        
        return v.rstrip('/')

    @field_validator('asset_id')
    @classmethod
    def validate_asset_id(cls, v: str) -> str:
        
        if not re.match(r"^[a-zA-Z0-9]+$", v):
            raise ValueError("Asset ID must be alphanumeric characters only.")
        return v
    
    @field_validator('record_id')
    @classmethod
    def validate_kobo_id(cls, v: str) -> str:
        
        if not v.isdigit():
            raise ValueError("The _id must contain only numbers.")
        
        if len(v) < 7 or len(v) > 8:
            raise ValueError("The _id length appears invalid for a Kobo record.")
            
        return v