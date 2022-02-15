"""
Generate a Json OpenAPI spec from Python docstring comments.

This script instantiates a Flask context for the similar_users app,
and extracts an apidocs.json file from the swagger blueprint.

Install dependencies and run from this repo top level with:
make venv
./doc/specgen.py -o apidocs.json
"""
from similar_users.factory import create_app
from similar_users.wsgi import swagger

import json
import os
import pathlib
import argparse

APIDOCS_OUTFILE = "apidocs.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a Json OpenAPI spec from Python docstring comments."
    )
    parser.add_argument(
        "--output",
        "-o",
        action="store",
        help=f"Output file (default: {APIDOCS_OUTFILE})",
        type=pathlib.Path,
        default=os.path.join(os.path.dirname(__file__), APIDOCS_OUTFILE),
    )
    return parser.parse_args()


def main(args):
    config = {
        "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:",
        "SQLALCHEMY_TRACK_MODIFICATIONS": False,
    }
    app = create_app(config)
    with app.app_context():
        with open(args.output, "w") as spec:
            spec.write(json.dumps(swagger.get_apispecs()))


if __name__ == "__main__":
    args = parse_args()
    main(args)
