import argparse
import pathlib
import os
import csv
import distutils
import uuid
import logging

from dataclasses import dataclass
from flask_sqlalchemy.model import DefaultMeta
from datetime import datetime
from itertools import islice

from similar_users.models import database, UserMetadata, Temporal, Coedit

# TODO(gmodena, 2020-12-07): we should use the factory constructor once 645072 is merged.
from similar_users.wsgi import app
from similar_users.wsgi import TIME_FORMAT
from similar_users.dblock import application_lock


@dataclass
class Source:
    resourcedir: str
    delimiter: str = "\t"


@dataclass
class TemporalSource(Source):
    file_name: str = "temporal.tsv"
    model: DefaultMeta = Temporal

    @staticmethod
    def map_record(row: dict) -> dict:
        """
        Map raw dataset column names to database model.

        :param row:
        :return:
        """
        return dict(
            user_text=row["user_text"],
            d=int(row["day_of_week"]) - 1,  # 0 Sunday - 6 Saturday
            h=int(row["hour_of_day"]),  # 0 - 23
            num_edits=int(row["num_edits"]),
        )


@dataclass
class MetadataSource(Source):
    file_name: str = "metadata.tsv"
    model: DefaultMeta = UserMetadata

    @staticmethod
    def map_record(row: dict) -> dict:
        """
        Map raw dataset column names to database model.

        :param row:
        :return:
        """
        return dict(
            user_text=row["user_text"],
            is_anon=bool(distutils.util.strtobool(row["is_anon"])),
            num_edits=int(row["num_edits"]),
            num_pages=int(row["num_pages"]),
            most_recent_edit=datetime.strptime(row["most_recent_edit"], TIME_FORMAT),
            oldest_edit=datetime.strptime(row["oldest_edit"], TIME_FORMAT),
        )


@dataclass
class CoeditSource(Source):
    file_name: str = "coedit_counts.tsv"
    model: DefaultMeta = Coedit

    @staticmethod
    def map_record(row: dict) -> dict:
        """
        Map raw dataset column names to database model.

        :param row:
        :return:
        """
        return dict(
            user_text=row["user_text"],
            user_text_neighbour=row["user_neighbor"],
            overlap_count=int(row["num_pages_overlapped"]),
        )


class Sink:
    def __init__(self, sources):
        """
        Manager the Similarusers service dataset updates.
        :param sources: a list of `Source` objects, mapping to datasets to insert
        """
        self.dataset_id = (
            uuid.uuid4()
        )  # unique identifier of the set of data being inserted
        self.sources = sources

    @staticmethod
    def _grouper(iterable, n):
        """
        Consume `n` items at a time from `iterable`

        >>>> _groupser('ABCDEFG', 2)
        >>>> (( 'A', 'B' ), ('C', 'D'), ('E', 'F'), ('G',) )

        :param iterable:
        :param n:
        :return:
        """
        it = iter(iterable)
        group = tuple(islice(it, n))
        while group:
            yield group
            group = tuple(islice(it, n))

    @application_lock
    def write(self, dry_run=False, batch_size=50):
        """
        Commit database changes, unless `dry_run` is `True`.

        :param dry_run:
        :param batch_size:
        :return:
        """
        try:
            for source in self.sources:
                self.load(source=source, batch_size=batch_size)
            if not dry_run:
                database.session.commit()
        except Exception as e:
            app.logger.error(f"Failed to commit transaction. Rolling back - {e}")
            if not dry_run:
                database.session.rollback()

    def load(self, source=None, batch_size=1):
        """
        Read from input dataset `source` and insert into the target database in
        bulks of size `batch_size`. Previously stored data will be deleted.

        Database changes are not committed; transaction management is delegated to
        the function caller.

        :param source:
        :param batch_size:
        :return:
        """
        app.logger.info(f"Loading {source.model.__name__} data")

        num_reads = 0
        num_skips = 0
        num_deleted = database.session.query(source.model).delete()

        insertion_metadata = {"dataset_id": str(self.dataset_id)}
        with open(os.path.join(source.resourcedir, source.file_name), "r") as infile:
            reader = csv.DictReader(
                infile, delimiter=source.delimiter, quoting=csv.QUOTE_NONE
            )
            for rows in self._grouper(reader, batch_size):
                mappings = []
                for row in rows:
                    num_reads += 1
                    try:
                        record = source.map_record(row)
                        record.update(insertion_metadata)
                    except Exception as e:
                        app.logger.error(f"Failed to parse record: {e}.\n{row}")
                        num_skips += 1
                    else:
                        mappings.append(record)
                database.session.bulk_insert_mappings(source.model, mappings)
            # TODO(gmodena, 2020-12-08): we could push these counters as metrics
            print(
                f"Model={source.model.__name__}\tDeleted={num_deleted}\tRead={num_reads}\tSkipped={num_skips}\tInserted={num_reads - num_skips}"
            )


def parse_args():
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(
        description="A script to ingest Similarusers datasets into mysql"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't commit changes database",
        dest="dry_run",
    )
    parser.add_argument(
        "--resourcedir",
        "-r",
        action="store",
        help="Path to the service input files",
        type=pathlib.Path,
        default=os.path.join(os.path.dirname(__file__), "resources"),
    )
    parser.add_argument(
        "--db-connection-string",
        action="store",
        help="Path to the service input files. When specified, data will be loaded "
        "into a database (default: in memory sqlite) ",
        type=str,
        dest="db_connection_string",
        default="sqlite:///:memory:",
    )
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Let SQLAlchemy create database tables. Useful for testing/dev, for production use "
        "cases we expect migrations to be managed separately.",
        dest="create_tables",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        dest="verbose",
        action="store_true",
        help="Verbose output.",
        default=False,
    )
    return parser.parse_args()


def main(args):
    logging.basicConfig(level=logging.WARNING)

    app.config["SQLALCHEMY_DATABASE_URI"] = args.db_connection_string
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    with app.test_request_context():
        database.init_app(
            app
        )  # TODO(gmodena, 2020-12-07): init_app can be delegated to
        # the factory constructor once 645072 is merged.
        if args.create_tables:
            database.create_all()
        sources = [
            TemporalSource(resourcedir=args.resourcedir),
            MetadataSource(resourcedir=args.resourcedir),
            CoeditSource(resourcedir=args.resourcedir),
        ]

        sink = Sink(sources=sources)
        sink.write()


if __name__ == "__main__":
    args = parse_args()
    main(args)
