"""In-process receipt issued only at the authenticated Streamlit account seam."""
from dataclasses import dataclass

_SEAL = object()


@dataclass(frozen=True)
class _Receipt:
    normalized_email: str
    seal: object
    owner_id: int


class _StreamlitIdentity(dict):
    __slots__ = ("_receipt",)


def issue_streamlit_identity(identity: dict, account: dict | None, *, auth_enabled: bool) -> dict:
    """Keep legacy mapping fields; the receipt is an attribute, never JSON data."""
    if type(identity) is not dict:
        return {}
    if auth_enabled is not True or type(account) is not dict:
        return identity
    if identity.get("channel") != "streamlit" or identity.get("is_anonymous") is not False:
        return identity
    email, account_email = identity.get("email"), account.get("email")
    if type(email) is not str or type(account_email) is not str:
        return identity
    normalized = email.strip().lower()
    if not normalized or normalized != account_email.strip().lower():
        return identity
    sealed = _StreamlitIdentity(identity)
    sealed._receipt = _Receipt(normalized, _SEAL, id(sealed))
    return sealed


def authenticated_email(identity: object) -> str | None:
    """A copied/JSON-decoded mapping or a foreign nominal subtype grants nothing."""
    if type(identity) is not _StreamlitIdentity:
        return None
    receipt = getattr(identity, "_receipt", None)
    if type(receipt) is not _Receipt or receipt.seal is not _SEAL:
        return None
    if type(receipt.owner_id) is not int or receipt.owner_id != id(identity):
        return None
    if identity.get("channel") != "streamlit" or identity.get("is_anonymous") is not False:
        return None
    email = identity.get("email")
    if (type(email) is not str or type(receipt.normalized_email) is not str
            or email.strip().lower() != receipt.normalized_email):
        return None
    return receipt.normalized_email
