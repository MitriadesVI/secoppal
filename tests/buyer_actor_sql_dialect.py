"""SQLite text emulation used by the synthetic Buyer-Actor source."""
import unicodedata


def unaccent(value):
    # Same NFD/combining-mark emulation as Contract Universe's database fixture.
    return None if value is None else ''.join(
        c for c in unicodedata.normalize('NFD', value) if not unicodedata.combining(c))
