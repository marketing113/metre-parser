from fastapi import FastAPI
from pydantic import BaseModel
from app.parser import parse_ods_from_url

app = FastAPI()

class ParseRequest(BaseModel):
    file_url: str

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/parse-metre-ods")
def parse(payload: ParseRequest):
    return parse_ods_from_url(payload.file_url)