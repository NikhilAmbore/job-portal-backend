"""
Remotive API scraper.
Free API, no key needed. Focuses on remote tech jobs.
API: https://remotive.com/api/remote-jobs
"""
import logging
from datetime import datetime, timezone, timedelta

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Remotive categories for tech jobs
TECH_CATEGORIES = [
    "software-dev",
    "data",
    "devops",
    "qa",
    "product",
    "design",
    "customer-support",  # often has technical support roles
]


class RemotiveScraper(BaseScraper):
    source_name = "remotive"

    def fetch_jobs(self) -> list[dict]:
        all_jobs = []

        for category in TECH_CATEGORIES:
            try:
                jobs = self._fetch_category(category)
                all_jobs.extend(jobs)
            except Exception as e:
                logger.error(f"Remotive: Error fetching '{category}': {e}")

        logger.info(f"Remotive: Fetched {len(all_jobs)} jobs")
        return all_jobs

    def _fetch_category(self, category: str) -> list[dict]:
        """Fetch remote jobs for a category."""
        url = "https://remotive.com/api/remote-jobs"
        params = {"category": category}

        resp = self.client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("jobs", [])
        jobs = []

        # Filter for jobs posted in last 48 hours (Remotive dates aren't always exact)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

        for item in results:
            pub_date = item.get("publication_date", "")
            if pub_date:
                try:
                    posted = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                    if posted < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass

            job = self._parse_job(item, category)
            if job:
                jobs.append(job)

        return jobs

    def _parse_job(self, item: dict, remotive_category: str) -> dict | None:
        """Parse a single Remotive job result."""
        try:
            title = item.get("title", "").strip()
            company = item.get("company_name", "Unknown")
            if not title:
                return None

            # Location — Remotive jobs are remote, but may have region restrictions
            candidate_location = item.get("candidate_required_location", "")
            city = None
            state = None
            location_str = candidate_location or "Remote"

            # Check if US-based
            if candidate_location:
                state = self.normalize_state(candidate_location)
                city = self.extract_city(candidate_location)

            # Description (HTML)
            description = item.get("description", "")
            import re
            description_text = re.sub(r"<[^>]+>", " ", description)
            description_text = re.sub(r"\s+", " ", description_text).strip()

            # Salary
            salary = item.get("salary", "")
            salary_min, salary_max = self.parse_salary(salary) if salary else (None, None)

            # Date
            pub_date = item.get("publication_date", "")
            posted_at = None
            if pub_date:
                try:
                    posted_at = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            # URL
            apply_url = item.get("url", "")

            # Logo
            logo = item.get("company_logo", None)

            # Tags
            tags = item.get("tags", [])

            # Category mapping
            cat_map = {
                "software-dev": "Software Engineering",
                "data": "Data Science & Analytics",
                "devops": "DevOps & Infrastructure",
                "qa": "Quality Assurance",
                "product": "Product & Project Management",
                "design": "UI/UX Design",
                "customer-support": "IT Operations & Support",
            }

            # Job type
            job_type = item.get("job_type", "")

            return {
                "external_id": str(item.get("id", hash(title + company))),
                "title": title,
                "company": company,
                "location_city": city,
                "location_state": state,
                "work_type": "remote",  # All Remotive jobs are remote
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": "USD",
                "experience_level": self.detect_experience(title, description_text),
                "category": cat_map.get(remotive_category, self.categorize(title, description_text)),
                "skills": tags[:10] if tags else [],
                "description": description_text[:5000],
                "apply_url": apply_url,
                "company_logo": logo,
                "source": self.source_name,
                "posted_at": posted_at or datetime.now(timezone.utc),
                "scraped_at": datetime.now(timezone.utc),
                "expires_at": None,
                "is_active": True,
            }
        except Exception as e:
            logger.error(f"Remotive: Error parsing job: {e}")
            return None
