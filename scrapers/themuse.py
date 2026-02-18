"""
The Muse API scraper.
Free API, no key needed. Features jobs from top tech companies.
API docs: https://www.themuse.com/developers/api/v2
"""
import time
import logging
from datetime import datetime, timezone, timedelta

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# The Muse categories relevant to tech/IT
TECH_CATEGORIES = [
    "Software Engineering",
    "Data Science",
    "Data and Analytics",
    "IT",
    "Design and UX",
    "Product",
    "Project Management",
]


class TheMuseScraper(BaseScraper):
    source_name = "themuse"

    def fetch_jobs(self) -> list[dict]:
        all_jobs = []

        for category in TECH_CATEGORIES:
            try:
                jobs = self._fetch_category(category)
                all_jobs.extend(jobs)
                time.sleep(1)
            except Exception as e:
                logger.error(f"TheMuse: Error fetching '{category}': {e}")

        logger.info(f"TheMuse: Fetched {len(all_jobs)} jobs")
        return all_jobs

    def _fetch_category(self, category: str) -> list[dict]:
        """Fetch jobs for a specific category, paginating through results."""
        jobs = []
        max_pages = 5  # Limit to avoid too many requests

        for page in range(max_pages):
            try:
                url = "https://www.themuse.com/api/public/jobs"
                params = {
                    "category": category,
                    "location": "United States",
                    "page": page,
                }

                resp = self.client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

                results = data.get("results", [])
                if not results:
                    break

                # Filter for jobs posted in last 24 hours
                cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

                for item in results:
                    pub_date = item.get("publication_date", "")
                    if pub_date:
                        try:
                            posted = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                            if posted < cutoff:
                                continue  # Skip old jobs
                        except (ValueError, TypeError):
                            pass

                    job = self._parse_job(item, category)
                    if job:
                        jobs.append(job)

                time.sleep(0.5)
            except Exception as e:
                logger.error(f"TheMuse: Error on page {page} of '{category}': {e}")
                break

        return jobs

    def _parse_job(self, item: dict, muse_category: str) -> dict | None:
        """Parse a single Muse job result."""
        try:
            title = item.get("name", "").strip()
            company_obj = item.get("company", {})
            company = company_obj.get("name", "Unknown")
            if not title:
                return None

            # Location
            locations = item.get("locations", [])
            city = None
            state = None
            location_str = ""
            work_type = "onsite"

            if locations:
                loc = locations[0].get("name", "")
                location_str = loc
                if "flexible" in loc.lower() or "remote" in loc.lower():
                    work_type = "remote"
                else:
                    city = self.extract_city(loc)
                    state = self.normalize_state(loc)

            # Description (HTML content)
            description = item.get("contents", "")
            import re
            description_text = re.sub(r"<[^>]+>", " ", description)
            description_text = re.sub(r"\s+", " ", description_text).strip()

            # Dates
            pub_date = item.get("publication_date", "")
            posted_at = None
            if pub_date:
                try:
                    posted_at = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            # Company logo
            logo = None
            refs = item.get("refs", {})
            if refs:
                logo = refs.get("logo_image")

            # Apply URL
            apply_url = refs.get("landing_page", "") if refs else ""

            # Map Muse categories to our categories
            category_map = {
                "Software Engineering": "Software Engineering",
                "Data Science": "Data Science & Analytics",
                "Data and Analytics": "Data Science & Analytics",
                "IT": "IT Operations & Support",
                "Design and UX": "UI/UX Design",
                "Product": "Product & Project Management",
                "Project Management": "Product & Project Management",
            }

            return {
                "external_id": str(item.get("id", hash(title + company))),
                "title": title,
                "company": company,
                "location_city": city,
                "location_state": state,
                "work_type": work_type if work_type != "onsite" else self.detect_work_type(title, description_text, location_str),
                "salary_min": None,
                "salary_max": None,
                "salary_currency": "USD",
                "experience_level": self.detect_experience(title, description_text),
                "category": category_map.get(muse_category, self.categorize(title, description_text)),
                "skills": [],
                "description": description_text[:5000],  # Cap length
                "apply_url": apply_url,
                "company_logo": logo,
                "source": self.source_name,
                "posted_at": posted_at or datetime.now(timezone.utc),
                "scraped_at": datetime.now(timezone.utc),
                "expires_at": None,
                "is_active": True,
            }
        except Exception as e:
            logger.error(f"TheMuse: Error parsing job: {e}")
            return None
