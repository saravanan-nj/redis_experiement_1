import pytest
from .db import DB


@pytest.fixture
def db_client():
    db = DB()
    yield db
    db.reset()


@pytest.fixture
def movies(db_client):
    values = [
        # partitionkey, sortkey, value
        ("genre:animated", "1", "How to train your dragon"),
        ("genre:scifi", "scifi011", "Inception"),
        ("genre:scifi", "scifi051", "Interstellar"),
        ("genre:horror", "190", "Conjuring"),
        ("genre:animated", "293", "Mega Mind"),
        ("genre:horror", "192", "Doctor Sleep"),
    ]
    for value in values:
        db_client.set_value("movies", *value)

    # print(db_client.keys())
    # print(db_client.dbsize())


def test_animated_movies(movies, db_client):

    # get animated movies
    animated_movies = db_client.get_range("movies", "genre:animated", "1", "1000")
    assert animated_movies == [
        ("How to train your dragon", "1"),
        ("Mega Mind", "293"),
    ]


def test_horror_movies(movies, db_client):

    # get horror movies
    horror_movies = db_client.get_range("movies", "genre:horror", "1", "1000")

    assert horror_movies == [
        ("Conjuring", "190"),
        ("Doctor Sleep", "192"),
    ]


def test_scifi_movies(movies, db_client):

    # get scifi movies
    scifi_movies = db_client.get_range("movies", "genre:scifi", "scifi000", "scifi999")
    assert scifi_movies == [("Inception", "scifi011"), ("Interstellar", "scifi051")]


def test_inception_movie(movies, db_client):
    assert db_client.get_value("movies", "genre:scifi", "scifi011") == "Inception"
    assert db_client.get_value("movies", "genre:horror", "scifi011") is None

