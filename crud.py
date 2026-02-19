"""
Database CRUD (Create, Read, Update, Delete) operations.
"""
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import func, text, or_, desc
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from models import Job


def upsert_job(db: Session, job_data: dict) -> bool:
    """
    Insert a job if it doesn't exist (based on source + external_id).
    Returns True if inserted, False if skipped (duplicate).
    """
    stmt = insert(Job).values(**job_data)
    stmt = stmt.on_conflict_do_nothing(index_elements=["source", "external_id"])
    result = db.execute(stmt)
    db.commit()
    return result.rowcount > 0


def upsert_jobs_bulk(db: Session, jobs: list[dict]) -> tuple[int, int]:
    """
    Bulk insert jobs, skipping duplicates.
    Returns (inserted_count, skipped_count).
    """
    if not jobs:
        return 0, 0

    inserted = 0
    for job_data in jobs:
        if upsert_job(db, job_data):
            inserted += 1

    return inserted, len(jobs) - inserted


def update_search_vectors(db: Session):
    """Update full-text search vectors for all jobs missing them."""
    db.execute(text("""
        UPDATE jobs
        SET search_vector = to_tsvector('english',
            coalesce(title, '') || ' ' ||
            coalesce(company, '') || ' ' ||
            coalesce(description, '') || ' ' ||
            coalesce(location_city, '') || ' ' ||
            coalesce(location_state, '')
        )
        WHERE search_vector IS NULL
    """))
    db.commit()


def get_jobs(
    db: Session,
    q: Optional[str] = None,
    category: Optional[str] = None,
    state: Optional[str] = None,
    work_type: Optional[str] = None,
    experience: Optional[str] = None,
    salary_min: Optional[int] = None,
    posted_within: Optional[str] = None,
    source: Optional[str] = None,
    page: int = 1,
    per_page: int = 20,
    sort: str = "posted_at",
) -> tuple[list[Job], int]:
    """
    Query jobs with filters, search, and pagination.
    Returns (jobs_list, total_count).
    """
    query = db.query(Job).filter(Job.is_active == True)

    # Full-text search — three-phase smart search
    # Phase 1: all words AND  →  Phase 2: domain words AND  →  Phase 3: domain words OR
    if q:
        # Generic job-title words that are too broad to drive OR expansion alone
        _GENERIC = {
            "developer","engineer","programmer","manager","analyst","architect",
            "specialist","consultant","coordinator","administrator","admin",
            "senior","junior","lead","staff","principal","associate","intern",
            "trainee","assistant","support","officer","director","head","chief",
            "member","team","software","technical","technology","tech","full","stack",
        }

        def _domain_words(text: str) -> list[str]:
            """Return only non-generic, meaningful words from the query."""
            return [w for w in text.strip().lower().split()
                    if len(w) > 2 and w not in _GENERIC]

        # Phase 1: strict AND — every word must appear
        and_tsq = func.plainto_tsquery("english", q)
        strict_count = (
            db.query(func.count(Job.id))
            .filter(Job.is_active == True, Job.search_vector.op("@@")(and_tsq))
            .scalar()
        )

        if strict_count > 0:
            # Exact match — use as-is
            query = query.filter(Job.search_vector.op("@@")(and_tsq))
        else:
            # Phase 2: domain-only AND — drop generic words, keep tech/domain terms
            domain_words = _domain_words(q)
            if domain_words:
                domain_and_phrase = " ".join(domain_words)
                domain_and_tsq = func.plainto_tsquery("english", domain_and_phrase)
                domain_and_count = (
                    db.query(func.count(Job.id))
                    .filter(Job.is_active == True, Job.search_vector.op("@@")(domain_and_tsq))
                    .scalar()
                )
                if domain_and_count > 0:
                    query = query.filter(Job.search_vector.op("@@")(domain_and_tsq))
                else:
                    # Phase 3: domain-only OR — any domain term matches
                    or_phrase = " OR ".join(domain_words)
                    or_tsq = func.websearch_to_tsquery("english", or_phrase)
                    query = query.filter(Job.search_vector.op("@@")(or_tsq))
            else:
                # All words were generic (e.g. "senior developer") — use full AND
                query = query.filter(Job.search_vector.op("@@")(and_tsq))

    # Filters
    if category:
        query = query.filter(Job.category == category)
    if state:
        query = query.filter(Job.location_state == state)
    if work_type:
        query = query.filter(Job.work_type == work_type)
    if experience:
        query = query.filter(Job.experience_level == experience)
    if salary_min:
        query = query.filter(
            or_(Job.salary_min >= salary_min, Job.salary_max >= salary_min)
        )
    if source:
        query = query.filter(Job.source == source)

    # Date filter
    if posted_within:
        days_map = {"1d": 1, "3d": 3, "7d": 7, "14d": 14, "30d": 30}
        days = days_map.get(posted_within, 30)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        query = query.filter(Job.posted_at >= cutoff)

    # Total count before pagination
    total = query.count()

    # Sorting
    if sort == "salary_max":
        query = query.order_by(desc(Job.salary_max).nulls_last())
    else:
        query = query.order_by(desc(Job.posted_at).nulls_last())

    # Pagination
    offset = (page - 1) * per_page
    jobs = query.offset(offset).limit(per_page).all()

    return jobs, total


def get_job_by_id(db: Session, job_id: UUID) -> Optional[Job]:
    """Get a single job by its UUID."""
    return db.query(Job).filter(Job.id == job_id, Job.is_active == True).first()


def get_categories_with_counts(db: Session) -> list[tuple[str, int]]:
    """Get all categories with their job counts."""
    return (
        db.query(Job.category, func.count(Job.id))
        .filter(Job.is_active == True, Job.category.isnot(None))
        .group_by(Job.category)
        .order_by(func.count(Job.id).desc())
        .all()
    )


def get_locations_with_counts(db: Session) -> list[tuple[str, int]]:
    """Get all states with their job counts."""
    return (
        db.query(Job.location_state, func.count(Job.id))
        .filter(Job.is_active == True, Job.location_state.isnot(None))
        .group_by(Job.location_state)
        .order_by(func.count(Job.id).desc())
        .all()
    )


def get_stats(db: Session) -> dict:
    """Get overall portal statistics."""
    total = db.query(func.count(Job.id)).scalar()
    active = db.query(func.count(Job.id)).filter(Job.is_active == True).scalar()
    companies = (
        db.query(func.count(func.distinct(Job.company)))
        .filter(Job.is_active == True)
        .scalar()
    )

    # Jobs per source
    sources = dict(
        db.query(Job.source, func.count(Job.id))
        .filter(Job.is_active == True)
        .group_by(Job.source)
        .all()
    )

    # Last scrape time
    last_scraped = db.query(func.max(Job.scraped_at)).scalar()

    return {
        "total_jobs": total,
        "active_jobs": active,
        "total_companies": companies,
        "sources": sources,
        "last_scraped": last_scraped,
    }


def expire_old_jobs(db: Session, days: int = 45):
    """Mark jobs older than X days as inactive."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    updated = (
        db.query(Job)
        .filter(Job.posted_at < cutoff, Job.is_active == True)
        .update({"is_active": False})
    )
    db.commit()
    return updated
