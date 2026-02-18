"""
USAJobs.gov API scraper.
Official US government job board — free API, no scraping needed.
Register at: https://developer.usajobs.gov/APIRequest/Index
"""
import os
import time
import logging
from datetime import datetime, timezone

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Tech/IT search keywords to cover all relevant positions
SEARCH_KEYWORDS = [
    "software engineer",
    "software developer",
    "data scientist",
    "data engineer",
    "cybersecurity",
    "cloud engineer",
    "devops",
    "IT specialist",
    "network engineer",
    "systems administrator",
    "database administrator",
    "web developer",
    "machine learning",
    "information security",
    "full stack developer",
]


class USAJobsScraper(BaseScraper):
    source_name = "usajobs"

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("USAJOBS_API_KEY", "")
        self.email = os.getenv("USAJOBS_EMAIL", "")

    def fetch_jobs(self) -> list[dict]:
        if not self.api_key or not self.email:
            logger.warning("USAJobs: Missing API key or email. Skipping.")
            return []

        all_jobs = []
        seen_ids = set()

        for keyword in SEARCH_KEYWORDS:
            try:
                jobs = self._search(keyword)
                for job in jobs:
                    if job["external_id"] not in seen_ids:
                        seen_ids.add(job["external_id"])
                        all_jobs.append(job)
                time.sleep(1)  # Rate limit: be respectful
            except Exception as e:
                logger.error(f"USAJobs: Error searching '{keyword}': {e}")

        logger.info(f"USAJobs: Fetched {len(all_jobs)} unique jobs")
        return all_jobs

    def _search(self, keyword: str) -> list[dict]:
        """Search USAJobs API for a keyword."""
        url = "https://data.usajobs.gov/api/search"
        headers = {
            "Authorization-Key": self.api_key,
            "User-Agent": self.email,
            "Host": "data.usajobs.gov",
        }
        params = {
            "Keyword": keyword,
            "DatePosted": 1,  # Last 24 hours
            "ResultsPerPage": 500,
            "Fields": "default",
        }

        resp = self.client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("SearchResult", {}).get("SearchResultItems", [])
        jobs = []

        for item in results:
            pos = item.get("MatchedObjectDescriptor", {})
            job = self._parse_job(pos)
            if job:
                jobs.append(job)

        return jobs

    def _parse_job(self, pos: dict) -> dict | None:
        """Parse a single USAJobs result into our standard format."""
        try:
            title = pos.get("PositionTitle", "")
            org = pos.get("OrganizationName", "")
            if not title or not org:
                return None

            # Location
            locations = pos.get("PositionLocation", [])
            location_str = ""
            city = None
            state = None
            if locations:
                loc = locations[0]
                location_str = loc.get("LocationName", "")
                city = loc.get("CityName")
                state_code = loc.get("CountrySubDivisionCode", "")
                if state_code and len(state_code) == 2:
                    from scrapers.base import US_STATES
                    state = US_STATES.get(state_code)

            if not city:
                city = self.extract_city(location_str)
            if not state:
                state = self.normalize_state(location_str)

            # Salary
            salary_min_str = pos.get("PositionRemuneration", [{}])[0].get("MinimumRange", "") if pos.get("PositionRemuneration") else ""
            salary_max_str = pos.get("PositionRemuneration", [{}])[0].get("MaximumRange", "") if pos.get("PositionRemuneration") else ""
            rate_type = pos.get("PositionRemuneration", [{}])[0].get("RateIntervalCode", "") if pos.get("PositionRemuneration") else ""

            salary_min = int(float(salary_min_str)) if salary_min_str else None
            salary_max = int(float(salary_max_str)) if salary_max_str else None

            # Convert hourly/biweekly to annual
            if rate_type == "Per Hour" and salary_min:
                salary_min = salary_min * 2080
                salary_max = (salary_max * 2080) if salary_max else salary_min
            elif rate_type == "Per Year":
                pass  # Already annual

            # Description
            desc = pos.get("UserArea", {}).get("Details", {}).get("MajorDuties", [""])[0] if pos.get("UserArea") else ""
            qual = pos.get("QualificationSummary", "")
            description = f"{desc}\n\n{qual}".strip() if desc else qual

            # Apply URL
            apply_url = pos.get("PositionURI", "") or pos.get("ApplyURI", [""])[0] if pos.get("ApplyURI") else ""

            # Dates
            start_date = pos.get("PositionStartDate", "")
            end_date = pos.get("PositionEndDate", "")
            posted_at = None
            expires_at = None
            if start_date:
                try:
                    posted_at = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass
            if end_date:
                try:
                    expires_at = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            # Work type
            schedule = pos.get("PositionSchedule", [{}])[0].get("Name", "") if pos.get("PositionSchedule") else ""
            work_type = self.detect_work_type(title, description, location_str)
            if "telework" in description.lower() or "remote" in location_str.lower():
                work_type = "remote"

            return {
                "external_id": pos.get("PositionID", str(hash(title + org))),
                "title": title,
                "company": org,
                "location_city": city,
                "location_state": state,
                "work_type": work_type,
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
                "expires_at": expires_at,
                "is_active": True,
            }
        except Exception as e:
            logger.error(f"USAJobs: Error parsing job: {e}")
            return None
