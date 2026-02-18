"""
Apify Google Jobs Scraper.
Uses Apify's Google Jobs actor to scrape public Google Jobs search results.
Google Jobs aggregates listings from LinkedIn, Indeed, Glassdoor, ZipRecruiter, and 20+ sites.

Register free at: https://apify.com (free tier = $5/month compute credits)
"""
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Focused search queries with US locations for maximum coverage
SEARCH_QUERIES = [
    "software engineer United States",
    "data scientist jobs USA",
    "devops engineer remote USA",
    "cybersecurity analyst United States",
    "cloud engineer AWS Azure",
    "full stack developer",
    "machine learning engineer",
    "frontend developer React",
    "backend developer Python Java",
    "QA engineer",
    "IT support specialist",
    "network engineer",
    "systems administrator",
    "data engineer",
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

        # Send ALL queries in a SINGLE actor run to save credits ($1 per run)
        logger.info(f"Apify: Running single batch with {len(SEARCH_QUERIES)} queries...")

        try:
            run_input = {
                "queries": SEARCH_QUERIES,
                "maxPagesPerQuery": 2,
                "csvFriendlyOutput": False,
                "languageCode": "en",
                "countryCode": "us",
            }

            run = client.actor("orgupdate/google-jobs-scraper").call(
                run_input=run_input,
                timeout_secs=300,
                memory_mbytes=512,
            )

            all_jobs = []
            seen_ids = set()
            item_count = 0

            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                item_count += 1
                job = self._parse_job(item)
                if job and job["external_id"] not in seen_ids:
                    seen_ids.add(job["external_id"])
                    all_jobs.append(job)

            logger.info(f"Apify: Actor returned {item_count} items, parsed {len(all_jobs)} unique jobs")
            return all_jobs

        except Exception as e:
            error_msg = str(e)
            if "usage" in error_msg.lower() or "limit" in error_msg.lower():
                logger.warning(f"Apify: Monthly credits exhausted. Skipping until next cycle.")
            else:
                logger.error(f"Apify: Error: {e}")
            return []

    def _parse_job(self, item: dict) -> dict | None:
        """Parse an Apify Google Jobs result into our standard format."""
        try:
            # Handle various field name formats from different actor versions
            title = (
                item.get("title") or
                item.get("job_title") or
                item.get("positionName") or
                ""
            ).strip()

            company = (
                item.get("companyName") or
                item.get("company_name") or
                item.get("company") or
                "Unknown"
            ).strip()

            if not title:
                return None

            # Location
            location_str = (
                item.get("location") or
                item.get("jobLocation") or
                ""
            )

            city = self.extract_city(location_str)
            state = self.normalize_state(location_str)

            # Salary
            salary_str = (
                item.get("salary") or
                item.get("salaryRange") or
                ""
            )
            salary_min, salary_max = self.parse_salary(salary_str)

            # Description
            description = (
                item.get("description") or
                item.get("job_description") or
                item.get("jobDescription") or
                ""
            )
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

            # Work type
            job_type = (
                item.get("jobType") or
                item.get("job_type") or
                item.get("employment_type") or
                item.get("employmentType") or
                ""
            )
            work_type = self.detect_work_type(title, description, location_str)
            if "remote" in job_type.lower():
                work_type = "remote"

            # Apply URL
            apply_url = (
                item.get("applyLink") or
                item.get("url") or
                item.get("apply_link") or
                item.get("link") or
                ""
            )
            # Some results have apply_options array
            apply_options = item.get("apply_options") or item.get("applyOptions") or []
            if apply_options and isinstance(apply_options, list) and len(apply_options) > 0:
                first_option = apply_options[0]
                if isinstance(first_option, dict):
                    apply_url = first_option.get("link") or first_option.get("url") or apply_url

            # Date posted
            date_str = (
                item.get("datePosted") or
                item.get("date") or
                item.get("date_posted") or
                item.get("postedAt") or
                ""
            )
            posted_at = self._parse_date(date_str)

            # Generate a stable external ID
            external_id = (
                item.get("id") or
                item.get("jobId") or
                item.get("job_id") or
                str(hash(f"{title}|{company}|{location_str}"))
            )

            # Company logo
            logo = item.get("companyLogo") or item.get("company_logo") or item.get("thumbnail") or None

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

        date_str = str(date_str).lower().strip()

        # Relative dates: "1 day ago", "2 hours ago", "just now"
        if "just now" in date_str or "today" in date_str:
            return datetime.now(timezone.utc)

        hour_match = re.search(r"(\d+)\s*hour", date_str)
        if hour_match:
            hours = int(hour_match.group(1))
            return datetime.now(timezone.utc) - timedelta(hours=hours)

        day_match = re.search(r"(\d+)\s*day", date_str)
        if day_match:
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
                clean = skill.replace("\\+", "+").replace("\\.", ".")
                found.append(clean)

        return found[:15]
