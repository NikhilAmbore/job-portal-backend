"""
SQLAlchemy database models.
"""
import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Text, Integer, Boolean, DateTime, Index, func
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY, TSVECTOR
from database import Base


class PageEvent(Base):
    __tablename__ = "analytics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event = Column(String(50), nullable=False)   # page_view | signup | login | session_start | resume_start
    page  = Column(String(100), nullable=True)   # index | app | resume | jobs | job_detail
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("idx_analytics_event", "event"),
        Index("idx_analytics_created", "created_at"),
    )


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    external_id = Column(String(255), nullable=False)
    title = Column(String(500), nullable=False)
    company = Column(String(300), nullable=False)
    location_city = Column(String(200), nullable=True)
    location_state = Column(String(100), nullable=True)
    work_type = Column(String(20), nullable=True)  # remote / hybrid / onsite
    salary_min = Column(Integer, nullable=True)
    salary_max = Column(Integer, nullable=True)
    salary_currency = Column(String(10), default="USD")
    experience_level = Column(String(50), nullable=True)  # entry / mid / senior / lead
    category = Column(String(100), nullable=True)
    skills = Column(ARRAY(Text), nullable=True)
    description = Column(Text, nullable=True)
    apply_url = Column(Text, nullable=True)
    company_logo = Column(Text, nullable=True)
    source = Column(String(50), nullable=False)  # usajobs / adzuna / themuse / remotive
    posted_at = Column(DateTime(timezone=True), nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Full-text search vector (auto-updated by trigger or manual)
    search_vector = Column(TSVECTOR, nullable=True)

    __table_args__ = (
        # Unique constraint: same job from same source can't be inserted twice
        Index("idx_jobs_source_external", "source", "external_id", unique=True),
        # Performance indexes
        Index("idx_jobs_posted", "posted_at"),
        Index("idx_jobs_category", "category"),
        Index("idx_jobs_state", "location_state"),
        Index("idx_jobs_active", "is_active"),
        Index("idx_jobs_work_type", "work_type"),
        # Full-text search index
        Index("idx_jobs_search", "search_vector", postgresql_using="gin"),
    )

    def __repr__(self):
        return f"<Job {self.title} @ {self.company}>"
