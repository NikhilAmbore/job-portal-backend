"""
Apify Google Jobs Scraper.
Uses Apify's Google Jobs actor to scrape public Google Jobs search results.
Google Jobs aggregates listings from LinkedIn, Indeed, Glassdoor, ZipRecruiter, and 20+ sites.

Register free at: https://apify.com (free tier = $5/month compute credits)
Actor: https://apify.com/orgupdate/google-jobs-scraper
"""
import os
import time
import logging
from datetime import datetime, timezone

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Tech job search queries to maximize coverage
SEARCH_QUERIES = [
    "software engineer",
    "data scientist",
    "data engineer",
    "devops engineer",
    "cloud engineer",
    "cybersecurity analyst",
    "full stack developer",
    "machine learning engineer",
    "frontend developer",
    "backend developer",
    "IT support specialist",
    "systems administrator",
    "network engineer",
    "QA engineer",
    "product manager tech",
]


class ApifyGoogleJobsScraper(BaseScraper):
    source_name = "google_jobs"

    def __init__(self):
        super().__init__()
        self.api_token = os.getenv("APIFY_API_TOKEN", "")

    def fetch_jobs(self) -> list[dict]:
        if not self.api_token:
            logger.warning("Apify: Missing API token. Skipping.")
            return []

        try:
            from apify_client import ApifyClient
        except ImportError:
            logger.error("Apify: apify-client not installed. Run: pip install apify-client")
            return []

        client = ApifyClient(self.api_token)
        all_jobs = []
        seen_ids = set()

        for keyword in SEARCH_QUERIES:
            try:
                logger.info(f"Apify: Searching '{keyword}'...")
                jobs = self._search(client, keyword)
                for job in jobs:
                    if job["external_id"] not in seen_ids:
                        seen_ids.add(job["external_id"])
                        all_jobs.append(job)

                # Respect Apify rate limits + free tier credits
                time.sleep(2)

            except Exception as e:
                logger.error(f"Apify: Error searching '{keyword}': {e}")

        logger.info(f"Apify: Fetched {len(all_jobs)} unique jobs from Google Jobs")
        return all_jobs

    def _search(self, client, keyword: str) -> list[dict]:
        """Run one Apify Google Jobs search."""
        run_input = {
            "countryName": "usa",
            "includeKeyword": keyword,
            "datePosted": "today",  # Only jobs posted today (last 24hrs)
            "pagesToFetch": 3,       # 3 pages per keyword (conserve credits)
        }

        # Run the actor and wait for completion (timeout 120s)
        run = client.actor("orgupdate/google-jobs-scraper").call(
            run_input=run_input,
            timeout_secs=120,
            memory_mbytes=256,
        )

        jobs = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            job = self._parse_job(item)
            if job:
                jobs.append(job)

        return jobs

    def _parse_job(self, item: dict) -> dict | None:
        """Parse an Apify Google Jobs result into our standard format."""
        try:
            title = (item.get("job_title") or item.get("title") or "").strip()
            company = (item.get("company_name") or item.get("company") or "Unknown").strip()
            if not title:
                return None

            # Location
            location_str = item.get("location") or ""
            city = self.extract_city(location_str)
            state = self.normalize_state(location_str)

            # Salary
            salary_str = item.get("salary") or ""
            salary_min, salary_max = self.parse_salary(salary_str)

            # Description
            description = item.get("description") or item.get("job_description") or ""
            import re
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

            # Work type
            job_type = item.get("job_type") or item.get("employment_type") or ""
            work_type = self.detect_work_type(title, description, location_str)
            if "remote" in job_type.lower():
                work_type = "remote"

            # Apply URL
            apply_url = item.get("url") or item.get("apply_link") or item.get("link") or ""
            # Some results have apply_options array
            apply_options = item.get("apply_options") or []
            if apply_options and isinstance(apply_options, list) and len(apply_options) > 0:
                apply_url = apply_options[0].get("link") or apply_url

            # Date posted
            date_str = item.get("date") or item.get("date_posted") or ""
            posted_at = self._parse_date(date_str)

            # Source via (e.g., "via LinkedIn", "via Indeed")
            posted_via = item.get("posted_via") or item.get("via") or "Google Jobs"

            # Generate a stable external ID
            external_id = item.get("id") or item.get("job_id") or str(
                hash(f"{title}|{company}|{location_str}")
            )

            # Company logo
            logo = item.get("company_logo") or item.get("thumbnail") or None

            return {
                "external_id": str(external_id),
                "title": title,
                "company": company,
                "location_city": city,
                "location_state": state,
                "work_type": work_type,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": "USD",
                "experience_level": self.detect_experience(title, description),
                "category": self.categorize(title, description),
                "skills": self._extract_skills(description),
                "description": description[:5000],
                "apply_url": apply_url,
                "company_logo": logo,
                "source": self.source_name,
                "posted_at": posted_at or datetime.now(timezone.utc),
                "scraped_at": datetime.now(timezone.utc),
                "expires_at": None,
                "is_active": True,
            }
        except Exception as e:
            logger.error(f"Apify: Error parsing job: {e}")
            return None

    def _parse_date(self, date_str: str) -> datetime | None:
        """Parse various date formats from Google Jobs."""
        if not date_str:
            return None

        date_str = date_str.lower().strip()

        # Relative dates: "1 day ago", "2 hours ago", "just now"
        import re
        if "just now" in date_str or "today" in date_str:
            return datetime.now(timezone.utc)

        hour_match = re.search(r"(\d+)\s*hour", date_str)
        if hour_match:
            from datetime import timedelta
            hours = int(hour_match.group(1))
            return datetime.now(timezone.utc) - timedelta(hours=hours)

        day_match = re.search(r"(\d+)\s*day", date_str)
        if day_match:
            from datetime import timedelta
            days = int(day_match.group(1))
            return datetime.now(timezone.utc) - timedelta(days=days)

        # Try ISO format
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

        return None

    def _extract_skills(self, description: str) -> list[str]:
        """Extract common tech skills from description."""
        import re
        skill_patterns = [
            "Python", "Java", "JavaScript", "TypeScript", "C\\+\\+", "C#", "Go", "Rust",
            "React", "Angular", "Vue", "Node\\.js", "Django", "Flask", "FastAPI", "Spring",
            "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Terraform",
            "SQL", "PostgreSQL", "MongoDB", "Redis", "Elasticsearch",
            "Git", "CI/CD", "Jenkins", "GitHub Actions",
            "REST", "GraphQL", "gRPC", "Kafka", "RabbitMQ",
            "TensorFlow", "PyTorch", "Scikit-learn", "Pandas",
            "Linux", "Bash", "Agile", "Scrum",
        ]

        found = []
        desc_text = description or ""
        for skill in skill_patterns:
            if re.search(r"\b" + skill + r"\b", desc_text, re.IGNORECASE):
                # Use proper casing from our list
                clean = skill.replace("\\+", "+").replace("\\.", ".")
                found.append(clean)

        return found[:15]  # Cap at 15 skills
