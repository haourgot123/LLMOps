from pydantic import BaseModel
from typing import List


class WebScrapingResponse(BaseModel):
    title: str
    url: str


class WebScrappingResponseExtractor(BaseModel):
    responses: List[WebScrapingResponse]