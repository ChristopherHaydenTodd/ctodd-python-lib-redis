#!/usr/bin/env python3
"""
    Purpose:
        Connector Library for Redis. Will provide a decorator for initiating a
        redis db connection and inject it as a parameter into a fuction.
"""

# Python Library Imports
import logging
import redis
import wrapt


def get_redis_connector(host, port=6379, password=None, db=0):
    """
        Purpose:
            Decorator for connecting to redis database
        Args:
            host (string): host of server
            password (string): password for redis
            port (int): port number for redis-server instance
            db (int): db instance in redis to connect to
        Returns:
            decorator (function): function decorating another
                function and injecting a redis_con for connection,
                committing, and closing the db connection
    """

    @wrapt.decorator
    def with_connection(f, instance, args, kwargs):
        """
            Purpose:
                Database connection wrapping function
            Args:
                f (function/method): function being decorated
                instance: pass in self when wraping class method.
                    default is None when wraping function.
                args (Tuple): List of arguments
                kwargs (Dict): Dictionary of named arguments
            Return:
                output (Object): Output of the wrapped Function
            Function Termination:
                Will close connection to the Redis database
        """

        redis_con = get_redis_connection(host, port=port, password=password, db=db)

        output = f(redis_con, *args, **kwargs)
        return output

    return with_connection


def get_redis_connection(host, port=6379, password=None, db=0):
    """
        Purpose:
            Get Redis Connection.
        Args:
            host (string): host of server
            password (string): password for redis
            port (int): port number for redis-server instance
            db (int): db instance in redis to connect to
        Returns:
            redis_con (StrictRedis): Redis Connection Object
    """
    logging.info(f"Connecting to Redis (db {db}) on {host}:{port}")

    try:
        if password:
            redis_con = redis.StrictRedis(
                host=host, port=port, db=db, password=password
            )
        else:
            redis_con = redis.StrictRedis(host=host, port=port, db=db)
        # Need to actually utilize connect to make sure it is connected
        redis_con.keys()
    except Exception as err:
        logging.exception(f"Error connecting to Redis: {err}")
        raise

    return redis_con
