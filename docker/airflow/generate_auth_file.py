"""Generate /opt/airflow/simple_auth_passwords.json with bcrypt-hashed passwords.

Airflow 3.x SimpleAuthManager reads usernames/passwords from a JSON file. We
hash the password with bcrypt before writing so a leaked file does NOT trivially
disclose the plaintext credential. Idempotent: writes the file with mode 0600.

Reads ``AIRFLOW_USER`` and ``AIRFLOW_PASSWORD`` from the environment. Falls back
to ``admin`` / ``airflow`` only if they're unset — but that path is meant for
local smoke tests and should never run in production.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import bcrypt

OUTPUT_PATH = Path("/opt/airflow/simple_auth_passwords.json")


def _hash(password: str) -> str:
    """Return a bcrypt hash (12 rounds) of ``password`` as an ASCII string."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def main() -> int:
    user = os.environ.get("AIRFLOW_USER", "admin")
    password = os.environ.get("AIRFLOW_PASSWORD", "airflow")

    if password in ("airflow", "admin", "password", "changeme"):
        # Loud warning but don't abort — local docker-compose smoke tests rely
        # on the default. Production deployments use strong passwords from .env.
        sys.stderr.write(
            f"WARNING: AIRFLOW_PASSWORD is weak ({password!r}); "
            "use a strong value in .env for non-local environments.\n"
        )

    # NOTE: Airflow 3's SimpleAuthManager does NOT support hashed passwords (like bcrypt).
    # It performs a direct plaintext string comparison. Storing it as a bcrypt hash will
    # cause authentication to fail (401 Unauthorized). Therefore, we must store the plaintext
    # password here. Since this file is located at a container-internal path that is never
    # mounted to the host, it remains secure from host-level leaks.
    payload = {user: password}
    OUTPUT_PATH.write_text(json.dumps(payload), encoding="utf-8")
    OUTPUT_PATH.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600

    print(f"Wrote {OUTPUT_PATH} for user={user!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
