"""
Centralized AWS access for every script in this project.

Import a client from here instead of calling boto3 directly, e.g.:

    from config.aws_session import get_client
    s3 = get_client("s3")
    s3.list_objects_v2(Bucket=...)

Why this file exists
--------------------
* One place decides the profile + region (from config/settings.py).
* Credentials are resolved from your local AWS profile ("joel-admin")
  -- they are NEVER hardcoded or committed.
* You can run any of these scripts straight from the VSCode integrated
  terminal, because that terminal inherits your AWS credentials.

Overrides (optional)
--------------------
* AWS_PROFILE / AWS_REGION env vars override the defaults in settings.py.
* AWS_CA_BUNDLE env var points boto3 at a corporate root-CA .pem if your
  network does TLS interception (same fix as the CLI's ca_bundle).
"""

import os
import boto3

from config import settings


# ============================================================
# SESSION -- built once and reused
# ============================================================
_session = None


def get_session() -> boto3.session.Session:
    """Return a cached boto3 Session using the configured profile/region."""
    global _session
    if _session is None:
        profile = os.environ.get("AWS_PROFILE", settings.AWS_PROFILE)
        region = os.environ.get("AWS_REGION", settings.AWS_REGION)
        _session = boto3.session.Session(
            profile_name=profile,
            region_name=region,
        )
    return _session


# ============================================================
# CLIENTS / RESOURCES
# ============================================================
def _tls_verify():
    """Return an AWS_CA_BUNDLE path if set, else True (default verification)."""
    return os.environ.get("AWS_CA_BUNDLE", True)


def get_client(service_name: str):
    """Return a boto3 client for the given service (e.g. 's3', 'glue')."""
    return get_session().client(service_name, verify=_tls_verify())


def get_resource(service_name: str):
    """Return a boto3 resource for the given service (e.g. 's3')."""
    return get_session().resource(service_name, verify=_tls_verify())


# ============================================================
# QUICK CONNECTIVITY CHECK
# ============================================================
def whoami() -> dict:
    """Return the caller identity -- handy 'am I authenticated?' check."""
    return get_client("sts").get_caller_identity()


if __name__ == "__main__":
    # Run `python -m config.aws_session` from the VSCode terminal to verify
    # your credentials work before running any of the real scripts.
    ident = whoami()
    print("Authenticated as:")
    print(f"  Account : {ident['Account']}")
    print(f"  ARN     : {ident['Arn']}")
    print(f"  Region  : {get_session().region_name}")
