import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session(max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """Create a shared requests session with retry/backoff on 429/5xx and connection errors.

    Centralized so all modules use identical networking behavior.
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"]),
        backoff_factor=backoff_factor,
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
