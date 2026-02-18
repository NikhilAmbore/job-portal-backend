"""
Adzuna API scraper.
Aggregates jobs from Indeed, Monster, and many other sources.
Register free at: https://developers.adzuna.com
Free tier: 500 requests/month.
"""
import os
import time
import logging
from datetime import datetime, timezone

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class AdzunaScraper(BaseScraper):
    source_name = "adzuna"

    def __init__(self):
        super().__init__()
        self.app_id = os.getenv("ADZUNA_APP_ID", "")
        self.app_key = os.getenv("ADZUNA_APP_KEY", "")

    def fetch_jobs(self) -> list[dict]:
        if not self.app_id or not self.app_key:
            logger.warning("Adzuna: Missing app_id or app_key. Skipping.")
            return []

        all_jobs = []
        max_pages = 5  # 50 per page * 5 pages = 250 jobs per run (conserve rate limit)

        for page in range(1, max_pages + 1):
            try:
                jobs = self._fetch_page(page)
                if not jobs:
                    break
                all_jobs.extend(jobs)
                time.sleep(2)  # Respect rate limits
            except Exception as e:
                logger.error(f"Adzuna: Error on page {page}: {e}")
                break

        logger.info(f"Adzuna: Fetched {len(all_jobs)} jobs")
        return all_jobs

    def _fetch_page(self, page: int) -> list[dict]:
        """Fetch one page of IT jobs from Adzuna."""
        url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": 50,
            "category": "it-jobs",
            "max_days_old": 1,
            "content-type": "application/json",
            "sort_by": "date",
        }

        resp = self.client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        jobs = []

        for item in results:
            job = self._parse_job(item)
            if job:
                jobs.append(job)

        return jobs

    def _parse_job(self, item: dict) -> dict | None:
        """Parse a single Adzuna result."""
        try:
            title = item.get("title", "").strip()
            company = item.get("company", {}).get("display_name", "Unknown")
            if not title:
                return None

            # Clean HTML from title
            import re
            title = re.sub(r"<[^>]+>", "", title)

            # Location
            location_str = item.get("location", {}).get("display_name", "")
            areas = item.get("location", {}).get("area", [])
            city = None
            state = None
            if len(areas) >= 3:
                state = areas[1] if areas[1] != "US" else None
                city = areas[-1] if len(areas) > 2 else None
            elif location_str:
                city = self.extract_city(location_str)
                state = self.normalize_state(location_str)

            # Salary
            salary_min = int(item.get("salary_min")) if item.get("salary_min") else None
            salary_max = int(item.get("salary_max")) if item.get("salary_max") else None

            # Description
            description = item.get("description", "")
            description = re.sub(r"<[^>]+>", "", description)  # Strip HTML

            # URL
            apply_url = item.get("redirect_url", "")

            # Date
            created = item.get("created", "")
            posted_at = None
            if created:
                try:
                    posted_at = datetime.fromisoformat(created.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            return {
                "external_id": str(item.get("id", hash(title + company))),
                "title": title,
                "company": company,
                "location_city": city,
                "location_state": state,
                "work_type": self.detect_work_type(title, description, location_str),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": "USD",
                "experience_level": self.detect_experience(title, description),
                "category": self.categorize(title, description),
                "skills": [],
                "description": description,
                "apply_url": apply_url,
                "company_logo": None,
                "source": self.source_name,
                "posted_at": posted_at or datetime.now(timezone.utc),
                "scraped_at": datetime.now(timezone.utc),
                "expires_at": None,
                "is_active": True,
            }
        except Exception as e:
            logger.error(f"Adzuna: Error parsing job: {e}")
            return None
