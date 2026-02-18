"""
24-hour job scraping scheduler.
Runs all scrapers automatically every day at 2:00 AM EST.
"""
import time
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from database import SessionLocal
from crud import upsert_jobs_bulk, update_search_vectors, expire_old_jobs

from scrapers.usajobs import USAJobsScraper
from scrapers.adzuna import AdzunaScraper
from scrapers.themuse import TheMuseScraper
from scrapers.remotive import RemotiveScraper
from scrapers.apify_google import ApifyGoogleJobsScraper

logger = logging.getLogger(__name__)

# Store last scrape results for the status endpoint
last_scrape_status = {
    "results": [],
    "started_at": None,
    "completed_at": None,
}


def run_all_scrapers():
    """
    Main scraping function — runs all 4 scrapers sequentially,
    inserts jobs into the database, deduplicates, and expires old jobs.
    """
    global last_scrape_status
    logger.info("=" * 60)
    logger.info("SCRAPE STARTED")
    logger.info("=" * 60)

    started_at = datetime.now(timezone.utc)
    results = []

    scrapers = [
        USAJobsScraper(),
        AdzunaScraper(),
        TheMuseScraper(),
        RemotiveScraper(),
        ApifyGoogleJobsScraper(),
    ]

    db = SessionLocal()

    try:
        for scraper in scrapers:
            scraper_start = time.time()
            source = scraper.source_name
            errors = 0

            try:
                logger.info(f"Running {source} scraper...")
                jobs = scraper.fetch_jobs()
                inserted, skipped = upsert_jobs_bulk(db, jobs)
                duration = round(time.time() - scraper_start, 1)

                result = {
                    "source": source,
                    "jobs_added": inserted,
                    "jobs_skipped": skipped,
                    "errors": 0,
                    "duration_seconds": duration,
                }
                results.append(result)
                logger.info(
                    f"{source}: +{inserted} new, {skipped} dupes, {duration}s"
                )
            except Exception as e:
                duration = round(time.time() - scraper_start, 1)
                results.append({
                    "source": source,
                    "jobs_added": 0,
                    "jobs_skipped": 0,
                    "errors": 1,
                    "duration_seconds": duration,
                })
                logger.error(f"{source}: FAILED — {e}")
            finally:
                scraper.close()

            # Small delay between scrapers
            time.sleep(3)

        # Update full-text search vectors
        logger.info("Updating search vectors...")
        update_search_vectors(db)

        # Expire old jobs (older than 45 days)
        expired = expire_old_jobs(db, days=45)
        if expired:
            logger.info(f"Expired {expired} old jobs")

    finally:
        db.close()

    completed_at = datetime.now(timezone.utc)
    total_added = sum(r["jobs_added"] for r in results)
    total_skipped = sum(r["jobs_skipped"] for r in results)
    total_errors = sum(r["errors"] for r in results)

    last_scrape_status = {
        "results": results,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
    }

    logger.info("=" * 60)
    logger.info(
        f"SCRAPE COMPLETE: +{total_added} new jobs, "
        f"{total_skipped} dupes, {total_errors} errors, "
        f"{round((completed_at - started_at).total_seconds(), 1)}s total"
    )
    logger.info("=" * 60)


def get_last_scrape_status() -> dict:
    """Return the status of the last scrape run."""
    return last_scrape_status


def create_scheduler() -> BackgroundScheduler:
    """
    Create and configure the APScheduler.
    Runs scraping daily at 2:00 AM US Eastern time.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_all_scrapers,
        trigger=CronTrigger(hour=2, minute=0, timezone="US/Eastern"),
        id="daily_scrape",
        name="Daily job scraping",
        replace_existing=True,
        misfire_grace_time=3600,  # Allow 1 hour grace period if missed
    )
    return scheduler
