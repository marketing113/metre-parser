from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel
from app.parser import parse_ods_from_url

app = FastAPI(title="Metre ODS Parser")


class ParseRequest(BaseModel):
    file_url: str
    chantier_id: Optional[str] = None
    type: Optional[str] = None
    version_index: Optional[int] = None


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/parse-metre-ods")
def parse(payload: ParseRequest):
    return parse_ods_from_url(
        file_url=payload.file_url,
        chantier_id=payload.chantier_id,
        metre_type=payload.type,
        version_index=payload.version_index,
    )
