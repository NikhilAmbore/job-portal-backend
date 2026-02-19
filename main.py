"""
Ambore Jobs — FastAPI Backend
Main application file with all API endpoints and scheduler startup.
"""
import os
import math
import logging
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, Depends, Query, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from dotenv import load_dotenv

load_dotenv()

from database import get_db, init_db
from crud import get_jobs, get_job_by_id, get_categories_with_counts, get_locations_with_counts, get_stats
from schemas import JobResponse, JobListResponse, CategoryCount, LocationCount, StatsResponse, ScrapeStatusResponse
from scheduler import create_scheduler, run_all_scrapers, get_last_scrape_status

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ADMIN_KEY = os.getenv("ADMIN_KEY", "change_this_to_a_random_string")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://ambore.org")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    logger.info("Initializing database...")
    init_db()
    logger.info("Database ready.")

    logger.info("Starting scheduler...")
    scheduler = create_scheduler()
    scheduler.start()
    logger.info("Scheduler started — scraping daily at 2:00 AM EST.")

    yield

    # Shutdown
    scheduler.shutdown()
    logger.info("Scheduler stopped.")


app = FastAPI(
    title="Ambore Jobs API",
    description="Tech & IT Job Portal API — aggregates jobs from USAJobs, Adzuna, The Muse, and Remotive.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "https://ambore.org",
        "https://incandescent-frangollo-6b34b1.netlify.app",
        "http://localhost:3000",
        "http://localhost:8080",
        "http://127.0.0.1:5500",   # VS Code Live Server
        "http://localhost:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════
# PUBLIC ENDPOINTS
# ═══════════════════════════════════════════

@app.get("/")
def root():
    """Health check."""
    return {"status": "ok", "service": "Ambore Jobs API", "version": "1.0.0"}


@app.get("/api/jobs", response_model=JobListResponse)
def list_jobs(
    q: Optional[str] = Query(None, description="Full-text search"),
    category: Optional[str] = Query(None, description="Filter by category"),
    state: Optional[str] = Query(None, description="Filter by state"),
    work_type: Optional[str] = Query(None, description="remote / hybrid / onsite"),
    experience: Optional[str] = Query(None, description="entry / mid / senior"),
    salary_min: Optional[int] = Query(None, description="Minimum salary filter"),
    posted_within: Optional[str] = Query(None, description="1d / 3d / 7d / 14d / 30d"),
    source: Optional[str] = Query(None, description="Filter by source"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Results per page"),
    sort: str = Query("posted_at", description="Sort by: posted_at or salary_max"),
    db: Session = Depends(get_db),
):
    """List jobs with search, filters, and pagination."""
    jobs, total = get_jobs(
        db,
        q=q,
        category=category,
        state=state,
        work_type=work_type,
        experience=experience,
        salary_min=salary_min,
        posted_within=posted_within,
        source=source,
        page=page,
        per_page=per_page,
        sort=sort,
    )

    return JobListResponse(
        jobs=[JobResponse.model_validate(j) for j in jobs],
        total=total,
        page=page,
        per_page=per_page,
        total_pages=math.ceil(total / per_page) if total > 0 else 0,
    )


@app.get("/api/jobs/categories", response_model=list[CategoryCount])
def list_categories(db: Session = Depends(get_db)):
    """Get all job categories with counts."""
    results = get_categories_with_counts(db)
    return [CategoryCount(category=cat, count=count) for cat, count in results]


@app.get("/api/jobs/locations", response_model=list[LocationCount])
def list_locations(db: Session = Depends(get_db)):
    """Get all states with job counts."""
    results = get_locations_with_counts(db)
    return [LocationCount(state=state, count=count) for state, count in results]


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
def get_single_job(job_id: UUID, db: Session = Depends(get_db)):
    """Get a single job by ID."""
    job = get_job_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse.model_validate(job)


@app.get("/api/stats", response_model=StatsResponse)
def portal_stats(db: Session = Depends(get_db)):
    """Get overall portal statistics."""
    stats = get_stats(db)
    categories = get_categories_with_counts(db)
    return StatsResponse(
        total_jobs=stats["total_jobs"],
        active_jobs=stats["active_jobs"],
        total_companies=stats["total_companies"],
        sources=stats["sources"],
        categories=[CategoryCount(category=cat, count=count) for cat, count in categories],
        last_scraped=stats["last_scraped"],
    )


# ═══════════════════════════════════════════
# ANALYTICS ENDPOINT (public — lightweight)
# ═══════════════════════════════════════════

@app.post("/api/track", status_code=204)
async def track_event(request: Request, db: Session = Depends(get_db)):
    """Receive a tracking event from the frontend."""
    from models import PageEvent
    try:
        body = await request.json()
        event = str(body.get("event", ""))[:50]
        page  = str(body.get("page",  ""))[:100]
        if event:
            db.add(PageEvent(event=event, page=page))
            db.commit()
    except Exception:
        pass  # never fail silently for analytics


# ═══════════════════════════════════════════
# ADMIN ENDPOINTS (protected by API key)
# ═══════════════════════════════════════════

def verify_admin(x_admin_key: str = Header(...)):
    """Check admin API key."""
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key")
    return True


@app.post("/api/admin/scrape")
def trigger_scrape(authorized: bool = Depends(verify_admin)):
    """Manually trigger a scrape of all sources."""
    import threading
    thread = threading.Thread(target=run_all_scrapers, daemon=True)
    thread.start()
    return {"status": "Scrape started in background", "message": "Check /api/admin/scrape/status for results"}


@app.get("/api/admin/scrape/status")
def scrape_status(authorized: bool = Depends(verify_admin)):
    """Check the status of the last scrape."""
    status = get_last_scrape_status()
    return status


@app.get("/api/admin/analytics")
def analytics_summary(authorized: bool = Depends(verify_admin), db: Session = Depends(get_db)):
    """Return analytics summary: total events grouped by event type and page."""
    from models import PageEvent
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)

    def count_events(since=None, event=None, page=None):
        q = db.query(func.count(PageEvent.id))
        if since:
            q = q.filter(PageEvent.created_at >= since)
        if event:
            q = q.filter(PageEvent.event == event)
        if page:
            q = q.filter(PageEvent.page == page)
        return q.scalar() or 0

    # Totals
    total        = count_events()
    today        = count_events(since=now.replace(hour=0, minute=0, second=0, microsecond=0))
    last_7_days  = count_events(since=now - timedelta(days=7))
    last_30_days = count_events(since=now - timedelta(days=30))

    # By event type
    rows = db.query(PageEvent.event, func.count(PageEvent.id)).group_by(PageEvent.event).order_by(func.count(PageEvent.id).desc()).all()
    by_event = {r[0]: r[1] for r in rows}

    # By page
    rows = db.query(PageEvent.page, func.count(PageEvent.id)).group_by(PageEvent.page).order_by(func.count(PageEvent.id).desc()).all()
    by_page = {r[0]: r[1] for r in rows}

    return {
        "total_events": total,
        "today": today,
        "last_7_days": last_7_days,
        "last_30_days": last_30_days,
        "by_event": by_event,
        "by_page": by_page,
    }


# ═══════════════════════════════════════════
# RUN (for local development)
# ═══════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
