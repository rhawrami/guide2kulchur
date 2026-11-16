"""Per-kind activity defaults and configurable rating/review gates."""
from dataclasses import dataclass


@dataclass(frozen=True)
class ActivityPolicy:
    min_ratings: int
    min_reviews: int
    mode: str

    def accepts(self, data):
        checks = []
        for field, minimum in (('rating_count', self.min_ratings), ('review_count', self.min_reviews)):
            if minimum > 0:
                value = data.get(field)
                checks.append(type(value) is int and value >= minimum)
        # Zero disables a threshold; disabling both explicitly accepts any record.
        return not checks or (any(checks) if self.mode == 'any' else all(checks))


def activity_policy(kind, min_ratings=None, min_reviews=None, mode=None):
    return ActivityPolicy(
        1 if min_ratings is None else min_ratings,
        (0 if kind == 'user' else 1) if min_reviews is None else min_reviews,
        ('all' if kind == 'user' else 'any') if mode is None else mode,
    )
