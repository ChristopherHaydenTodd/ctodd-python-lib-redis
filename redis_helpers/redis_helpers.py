#!/usr/bin/env python3
"""
    Purpose:
        Helper Library for Redis. Will provide a functions for interacting with Redis.
        Includes wrapping calls to the DB with checks and balances for
        preventing mismanagement.
"""

# Python Library Imports
import logging
import json


###
# DB Management
###


def run_flushdb(redis_con):
    """
        Purpose:
            Run Redis FlushDB Command. Will clear all
            keys and values from the Redis Database (good
            for clearing cache when redis is used as
            a caching solution)
        Args:
            redis_con (Redis StrictRedis): Connection
                to Redis database
        Return
            was_successful (bool): whether or not the
                command was successful running
    """
    logging.info("Flushing Redis DB")

    try:
        redis_con.flushdb()
    except Exception as err:
        logging.error("Error Flushing DB: {0}".format(err))
        return False

    return True


###
# Managing Keys
###


def get_keys_matching_pattern(redis_con, pattern="*"):
    """
        Purpose:
            Get keys matching specified pattern. Default is all
            keys in the redis instance
        Args:
            redis_con (Redis StrictRedis): Connection
                to Redis database
            pattern (Regex/String): Regex or String for deleting
                keys in Redis Database
        Return
            keys (List of Strings): list of keys matching specified pattern
    """
    logging.info("Getting Keys from Redis Database: {pattern}".format(pattern=pattern))

    return list(redis_con.keys(pattern))


def delete_keys_matching_pattern(redis_con, pattern):
    """
        Purpose:
            Run Delete all keys matching a specified pattern
        Args:
            redis_con (Redis StrictRedis): Connection
                to Redis database
            pattern (Regex/String): Regex or String for deleting
                keys in Redis Database
        Return
            was_successful (bool): whether or not the
                command was successful running
    """
    logging.info("Delete Keys Matching Pattern: {0}".format(pattern))

    keys = get_keys_matching_pattern(redis_con, pattern=pattern)
    pipe = redis_con.pipeline()
    for key in keys:
        pipe.delete(key)

    try:
        values = pipe.execute()
    except Exception as err:
        logging.error("Exception Deleting Keys: {0}".format(err))
        return False

    if False in values:
        return False
    else:
        return True


###
# Single Value Key (String Keys) Functions
###


def set_value_of_single_value_key(
    redis_con, key, value, overwrite=False, value_format=None, value_encode=None
):
    """
        Purpose:
            Set data for key holding single value (stored as strings).

            If overwrite is false, do not overwrite already existing key.
            If it is true, set value regardless of if the key exists in
            the db
        Args:
            redis_con (Redis StrictRedis): Connection
                to Redis database
            key (Object): key to set in Redis
            value (Object): value to set in Redis for the specified key
            value_format (string): form of value to convert to string.
                Currently supports json via json.loads. But can be expanded
            value_encode (string): encoding type for the value
        Return
            was_successful (bool): whether or not the
                command was successful running
    """
    logging.info(
        "Setting Key {0}: {1} (overwrite set to {2})".format(key, value, overwrite)
    )

    existing_value = redis_con.get(key)
    if existing_value and not overwrite:
        logging.info(
            "Not Setting Value. Key already exists and overwrite set" " to False"
        )
        return False

    if value_format == "json":
        value = json.dumps(value)
    if value_encode:
        value = value.encode("utf-8")

    try:
        redis_con.set(key, value)
    except Exception as err:
        logging.error("Error Setting Key {0}: {1}".format(key, value))
        return False

    return True


def get_value_of_single_value_key(redis_con, key, value_format=None, value_decode=None):
    """
        Purpose:
            Get data from corresponding single-value key passed in (stored
            as strings and will be decoded if necessary).
        Args:
            redis_con (Redis StrictRedis): Connection
                to Redis database
            key (string): key to get data from Redis
            value_format (string): form to return value. Currently
                supports json via json.loads. But can be expanded
            value_decode (string): encoding type for the value
        Return
            value (Object): value stored in the passed
                in key as an object
    """
    logging.info("Getting Data from Redis Database for key: {key}".format(key=key))

    value = redis_con.get(key)
    if value_decode:
        value = value.decode("utf-8")
    if value_format == "json":
        value = json.loads(value)

    return value


###
# List Key Functions
###


def add_value_to_list_key(redis_con, key, value, value_format=None, value_encode=None):
    """
        Purpose:
            add value to key holding list
        Args:
            redis_con (Redis StrictRedis): Connection
                to Redis database
            key (Object): key to set in Redis
            value (Object): value to set in Redis for the specified key
        Return
            was_successful (bool): whether or not the
                command was successful running
    """
    logging.info("Appending Value to Key {0}: {1}".format(key, value))

    if value_format == "json":
        value = json.dumps(value)
    if value_encode:
        value = value.encode("utf-8")

    try:
        redis_con.lpush(key, value)
    except Exception as err:
        logging.error("Error Setting Key {0}: {1}".format(key, value))
        return False

    return True
