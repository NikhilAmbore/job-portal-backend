"""
Pydantic schemas for API request/response validation.
"""
from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel


class JobResponse(BaseModel):
    """Single job in API responses."""
    id: UUID
    title: str
    company: str
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    work_type: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    experience_level: Optional[str] = None
    category: Optional[str] = None
    skills: Optional[list[str]] = None
    description: Optional[str] = None
    apply_url: Optional[str] = None
    company_logo: Optional[str] = None
    source: str
    posted_at: Optional[datetime] = None
    is_active: bool = True

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    """Paginated job list response."""
    jobs: list[JobResponse]
    total: int
    page: int
    per_page: int
    total_pages: int


class CategoryCount(BaseModel):
    category: str
    count: int


class LocationCount(BaseModel):
    state: str
    count: int


class StatsResponse(BaseModel):
    total_jobs: int
    active_jobs: int
    total_companies: int
    sources: dict[str, int]
    categories: list[CategoryCount]
    last_scraped: Optional[datetime] = None


class ScrapeResult(BaseModel):
    source: str
    jobs_added: int
    jobs_skipped: int
    errors: int
    duration_seconds: float


class ScrapeStatusResponse(BaseModel):
    results: list[ScrapeResult]
    total_added: int
    total_skipped: int
    total_errors: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
