import os

import nox

FIX = os.environ.get("FIX", "0") == "1"


@nox.session
def lint(session: nox.Session):
    session.install("-r", "dev-requirements.txt")
    session.install("-e", ".")
    if not FIX:
        session.run("black", ".", "--check")
        session.run("isort", "--check", "--diff", ".")
    else:
        session.run("black", ".")
        session.run("isort", ".")


@nox.session
def tests(session):
    session.install("-r", "dev-requirements.txt")
    session.install("-e", ".")

    session.run("pytest")
