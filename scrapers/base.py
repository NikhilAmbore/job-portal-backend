"""
Base scraper class — all scrapers inherit from this.
Provides common utilities for fetching, parsing, and categorizing jobs.
"""
import re
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Job category mapping: keyword patterns → category name
CATEGORY_RULES = [
    (r"(?i)\b(machine learning|ml engineer|ai engineer|deep learning|nlp|computer vision)\b", "AI & Machine Learning"),
    (r"(?i)\b(data scien|data analy|analytics|business intelligence|bi developer)\b", "Data Science & Analytics"),
    (r"(?i)\b(data engineer|etl|data pipeline|data platform|dbt|airflow)\b", "Data Engineering"),
    (r"(?i)\b(devops|sre|site reliability|infrastructure|platform engineer|cicd|ci/cd)\b", "DevOps & Infrastructure"),
    (r"(?i)\b(cyber|security|infosec|soc analyst|penetration|vulnerability)\b", "Cybersecurity"),
    (r"(?i)\b(cloud|aws|azure|gcp|cloud engineer|cloud architect)\b", "Cloud Computing"),
    (r"(?i)\b(frontend|front-end|react|angular|vue|ui developer)\b", "Frontend Development"),
    (r"(?i)\b(backend|back-end|server-side|api developer)\b", "Backend Development"),
    (r"(?i)\b(full.?stack|fullstack)\b", "Full Stack Development"),
    (r"(?i)\b(mobile|ios|android|swift|kotlin|flutter|react native)\b", "Mobile Development"),
    (r"(?i)\b(qa|quality assurance|test engineer|sdet|automation test)\b", "Quality Assurance"),
    (r"(?i)\b(network|systems admin|it support|helpdesk|desktop support|it specialist)\b", "IT Operations & Support"),
    (r"(?i)\b(product manager|program manager|project manager|scrum master|agile)\b", "Product & Project Management"),
    (r"(?i)\b(ui/ux|ux design|ui design|user experience|user interface|product design)\b", "UI/UX Design"),
    (r"(?i)\b(software engineer|software developer|developer|programmer|swe)\b", "Software Engineering"),
    (r"(?i)\b(database|sql|dba|database admin)\b", "Database Administration"),
    (r"(?i)\b(embedded|firmware|hardware|iot)\b", "Embedded & IoT"),
    (r"(?i)\b(blockchain|web3|crypto|solidity)\b", "Blockchain & Web3"),
]

# Experience level detection
EXPERIENCE_RULES = [
    (r"(?i)\b(intern|internship|co-op)\b", "intern"),
    (r"(?i)\b(entry.level|junior|jr\.|associate|new grad|graduate)\b", "entry"),
    (r"(?i)\b(mid.level|mid-senior|intermediate)\b", "mid"),
    (r"(?i)\b(senior|sr\.|lead|staff|principal)\b", "senior"),
    (r"(?i)\b(director|vp|vice president|head of|chief|cto|cio)\b", "executive"),
]

# Work type detection
WORK_TYPE_RULES = [
    (r"(?i)\b(remote|work from home|wfh|anywhere|distributed)\b", "remote"),
    (r"(?i)\b(hybrid|flexible|partly remote)\b", "hybrid"),
    (r"(?i)\b(on.?site|in.?office|in-person)\b", "onsite"),
]

# US state abbreviation to full name mapping
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}
STATE_ABBREVS = {v.lower(): v for v in US_STATES.values()}
STATE_ABBREVS.update({k.lower(): US_STATES[k] for k in US_STATES})


class BaseScraper:
    """Base class for all job API scrapers."""

    source_name: str = "unknown"

    def __init__(self):
        self.client = httpx.Client(timeout=30, follow_redirects=True)

    def close(self):
        self.client.close()

    def fetch_jobs(self) -> list[dict]:
        """Override in subclass. Returns list of normalized job dicts."""
        raise NotImplementedError

    def categorize(self, title: str, description: str = "") -> str:
        """Auto-detect job category from title and description."""
        text = f"{title} {description}"
        for pattern, category in CATEGORY_RULES:
            if re.search(pattern, text):
                return category
        return "Other Tech"

    def detect_experience(self, title: str, description: str = "") -> Optional[str]:
        """Detect experience level from title/description."""
        text = f"{title} {description}"
        for pattern, level in EXPERIENCE_RULES:
            if re.search(pattern, text):
                return level
        return "mid"  # default

    def detect_work_type(self, title: str, description: str = "", location: str = "") -> str:
        """Detect remote/hybrid/onsite from title, description, or location."""
        text = f"{title} {description} {location}"
        for pattern, wtype in WORK_TYPE_RULES:
            if re.search(pattern, text):
                return wtype
        return "onsite"  # default

    def normalize_state(self, location: str) -> Optional[str]:
        """Extract US state from a location string."""
        if not location:
            return None
        # Try to find state abbreviation (e.g., "San Francisco, CA")
        match = re.search(r",\s*([A-Z]{2})\b", location)
        if match:
            abbrev = match.group(1)
            if abbrev in US_STATES:
                return US_STATES[abbrev]
        # Try full state name
        loc_lower = location.lower()
        for key, state in STATE_ABBREVS.items():
            if key in loc_lower:
                return state
        return None

    def extract_city(self, location: str) -> Optional[str]:
        """Extract city from a location string like 'San Francisco, CA'."""
        if not location:
            return None
        parts = location.split(",")
        if parts:
            city = parts[0].strip()
            # Don't return state names or "United States" as city
            if city.lower() not in STATE_ABBREVS and city.lower() != "united states":
                return city
        return None

    def parse_salary(self, salary_str: str) -> tuple[Optional[int], Optional[int]]:
        """Parse salary string into (min, max) integers."""
        if not salary_str:
            return None, None
        # Find all numbers in the string
        numbers = re.findall(r'[\d,]+\.?\d*', salary_str.replace(",", ""))
        numbers = [int(float(n)) for n in numbers if float(n) > 100]  # filter out non-salary numbers

        # Handle hourly rates (multiply by 2080 for annual)
        if re.search(r"(?i)(hour|hr|/hr|per hour)", salary_str):
            numbers = [n * 2080 for n in numbers]

        if len(numbers) >= 2:
            return min(numbers), max(numbers)
        elif len(numbers) == 1:
            return numbers[0], numbers[0]
        return None, None

    def dedup_hash(self, title: str, company: str, location: str = "") -> str:
        """Generate a hash for cross-source deduplication."""
        normalized = f"{title.lower().strip()}|{company.lower().strip()}|{location.lower().strip()}"
        return hashlib.md5(normalized.encode()).hexdigest()
