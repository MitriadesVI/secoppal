"""Post-ground integrity for a complete observation; no repair or interpretation."""
from .models import GroundedJob


def validate_job_uniqueness(jobs: tuple[GroundedJob, ...]) -> None:
    for index, job in enumerate(jobs):
        for prior in jobs[:index]:
            overlaps = max(job.start, prior.start) < min(job.end, prior.end)
            same_pair = (job.action, job.aspect) == (prior.action, prior.aspect)
            if overlaps and (same_pair or job.action == "OTHER" or prior.action == "OTHER"):
                raise ValueError("DUPLICATE_OR_OVERLAPPING_JOB")
