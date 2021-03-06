"""
Data models for the sockpuppet api.

Currently the schemas are a one-to-one mapping of fixtures produced
in development.
"""

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

database = SQLAlchemy()


class UserMetadata(database.Model):
    """
    Represents attributes for users in Coedit.
    """
    __tablename__ = "user"
    id = database.Column(database.Integer, primary_key=True)
    user_text = database.Column(database.String)
    is_anon = database.Column(database.Boolean)
    num_edits = database.Column(database.Integer)
    num_pages = database.Column(database.Integer)
    most_recent_edit = database.Column(database.DateTime)
    oldest_edit = database.Column(database.DateTime)
    insertion_time = database.Column(database.DateTime, server_default=func.current_timestamp())
    dataset_id = database.Column(database.String)


class Coedit(database.Model):
    """
    Represents a (user, user) similarity matrix in terms of number
    of edits in which two users overlapped.
    """
    __tablename__ = "coedit"
    id = database.Column(database.Integer, primary_key=True)
    user_text = database.Column(database.String)
    user_text_neighbour = database.Column(database.String)
    overlap_count = database.Column(database.Integer)
    insertion_time = database.Column(database.DateTime, server_default=func.current_timestamp())
    dataset_id = database.Column(database.String)


class Temporal(database.Model):
    """
    Represents temporal information about Coedit users editing behaviour - that is, when
    edits occur.
    """
    __tablename__ = "temporal"
    id = database.Column(database.Integer, primary_key=True)
    user_text = database.Column(database.String)
    d = database.Column(database.Integer)
    h = database.Column(database.Integer)
    num_edits = database.Column(database.Integer)
    insertion_time = database.Column(database.DateTime, server_default=func.current_timestamp())
    dataset_id = database.Column(database.String)
