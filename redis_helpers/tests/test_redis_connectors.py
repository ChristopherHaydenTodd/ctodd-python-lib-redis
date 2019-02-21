#!/usr/bin/env python3
"""
    Purpose:
        Test File for redis_connectors.py
"""

# Python Library Imports
import fakeredis
import json
import os
import pytest
import sys
import time
import timeout_decorator
from redis.exceptions import ConnectionError, ResponseError
from timeout_decorator.timeout_decorator import TimeoutError
from unittest import mock

# Import File to Test
from redis_helpers import redis_connectors


###
# Fixtures
###


#
# Fake Redis Con
#


@pytest.fixture
def fake_redis_con():
    """
    Purpose:
        Create Fake Redis Connection To Test With
    Args:
        N/A
    Return:
        fake_redis_con (Pytest Fixture (FakeRedis Connection Obj)): Fake redis connection
            that simulates redis functionality for testing
    """

    return fakeredis.FakeStrictRedis()


@pytest.fixture(autouse=True)
def reset_fake_redis_con(fake_redis_con):
    """
    Purpose:
        Flush the fake_redis_con keys after each test so that one test to another does
        not affect each other.
    Args:
        fake_redis_con (Pytest Fixture (FakeRedis Connection Obj)): Fake redis connection
            that simulates redis functionality for testing
    Return:
        N/A
    """

    fake_redis_con.flushdb()


#
# Connecting
#


@pytest.fixture
def example_host():
    """
    Purpose:
        Set example host for redis
    Args:
        N/A
    Return:
        example_host (Pytest Fixture (String)): example host for redis
    """

    return "redis_host"


@pytest.fixture
def example_port():
    """
    Purpose:
        Set example port for redis
    Args:
        N/A
    Return:
        example_port (Pytest Fixture (Int)): example port for redis
    """

    return 46379


@pytest.fixture
def default_port():
    """
    Purpose:
        Set default port for redis
    Args:
        N/A
    Return:
        default_port (Pytest Fixture (Int)): default port for redis
    """

    return 6379


@pytest.fixture
def example_password():
    """
    Purpose:
        Set example password for redis
    Args:
        N/A
    Return:
        example_password (Pytest Fixture (String)): example password for redis
    """

    return "redis_password"


@pytest.fixture
def default_password():
    """
    Purpose:
        Set default password for redis
    Args:
        N/A
    Return:
        default_password (Pytest Fixture (String)): default password for redis
    """

    return None


@pytest.fixture
def example_db():
    """
    Purpose:
        Set example db for redis
    Args:
        N/A
    Return:
        example_db (Pytest Fixture (Int)): example db for redis
    """

    return 7


@pytest.fixture
def default_db():
    """
    Purpose:
        Set default db for redis
    Args:
        N/A
    Return:
        default_db (Pytest Fixture (Int)): default db for redis
    """

    return 0


###
# Mocked Functions
###


# Utilizing FakeRedis for Mocking


###
# Test Connecting to Redis
###


def test_get_redis_connection_redis_down(example_host):
    """
    Purpose:
        Tests that redis_connectors.get_redis_connection() throws a ConnectionError exception
        when there is no service (not using mock here, really want to try and connect
        to something that isn't there)
    Args:
        example_host (Pytest Fixture (String)): example host for redis
    Return:
        N/A
    """

    with pytest.raises(ConnectionError):
        redis_con = redis_connectors.get_redis_connection(example_host)


@mock.patch("redis.StrictRedis", autospec=fakeredis.FakeStrictRedis)
def test_get_redis_connection_no_host(mocked_class):
    """
    Purpose:
        Tests that redis_connectors.get_redis_connection() requires at least a host. It
        should throw a TypeError if not
    Args:
        mocked_class (Mocked Class): Mocked version of redis.StrictRedis
    Return:
        N/A
    """

    with pytest.raises(TypeError):
        redis_con = redis_connectors.get_redis_connection()


@mock.patch("redis.StrictRedis", autospec=fakeredis.FakeStrictRedis)
def test_get_redis_connection_only_host(
    mocked_class, example_host, default_port, default_password, default_db
):
    """
    Purpose:
        Tests that redis_connectors.get_redis_connection() requires at least a host
    Args:
        mocked_class (Mocked Class): Mocked version of redis.StrictRedis
        example_host (Pytest Fixture (String)): example host for redis
        default_port (Pytest Fixture (Int)): default port for redis
        default_password (Pytest Fixture (String)): default password for redis
        default_db (Pytest Fixture (Int)): default db for redis
    Return:
        N/A
    """

    # Test Call
    redis_con = redis_connectors.get_redis_connection(example_host)

    # Assertions
    mocked_class.assert_called_with(host=example_host, port=default_port, db=default_db)
    mocked_class.assert_called_once()


@mock.patch("redis.StrictRedis", autospec=fakeredis.FakeStrictRedis)
def test_get_redis_connection_with_port(
    mocked_class, example_host, example_port, default_password, default_db
):
    """
    Purpose:
        Tests that redis_connectors.get_redis_connection() will call StrictRedis a specified
        port if one is provided
    Args:
        mocked_class (Mocked Class): Mocked version of redis.StrictRedis
        example_host (Pytest Fixture (String)): example host for redis
        example_port (Pytest Fixture (Int)): example port for redis
        default_password (Pytest Fixture (String)): default password for redis
        default_db (Pytest Fixture (Int)): default db for redis
    Return:
        N/A
    """

    # Test Call
    redis_con = redis_connectors.get_redis_connection(example_host, port=example_port)

    # Assertions
    mocked_class.assert_called_with(host=example_host, port=example_port, db=default_db)
    mocked_class.assert_called_once()


@mock.patch("redis.StrictRedis", autospec=fakeredis.FakeStrictRedis)
def test_get_redis_connection_with_db(
    mocked_class, example_host, default_port, default_password, example_db
):
    """
    Purpose:
        Tests that redis_connectors.get_redis_connection() will call StrictRedis a specified
        db if one is provided
    Args:
        mocked_class (Mocked Class): Mocked version of redis.StrictRedis
        example_host (Pytest Fixture (String)): example host for redis
        default_port (Pytest Fixture (Int)): default port for redis
        default_password (Pytest Fixture (String)): default password for redis
        example_db (Pytest Fixture (Int)): example db for redis
    Return:
        N/A
    """

    # Test Call
    redis_con = redis_connectors.get_redis_connection(example_host, db=example_db)

    # Assertions
    mocked_class.assert_called_with(host=example_host, port=default_port, db=example_db)
    mocked_class.assert_called_once()


@mock.patch("redis.StrictRedis", autospec=fakeredis.FakeStrictRedis)
def test_get_redis_connection_with_password(
    mocked_class, example_host, default_port, example_password, default_db
):
    """
    Purpose:
        Tests that redis_connectors.get_redis_connection() will call StrictRedis with a
        password if one is provided
    Args:
        mocked_class (Mocked Class): Mocked version of redis.StrictRedis
        example_host (Pytest Fixture (String)): example host for redis
        default_port (Pytest Fixture (Int)): default port for redis
        example_password (Pytest Fixture (String)): example password for redis
        default_db (Pytest Fixture (Int)): default db for redis
    Return:
        N/A
    """

    # Test Call
    redis_con = redis_connectors.get_redis_connection(
        example_host, password=example_password
    )

    # Assertions
    mocked_class.assert_called_with(
        host=example_host, port=default_port, password=example_password, db=default_db
    )
    mocked_class.assert_called_once()
