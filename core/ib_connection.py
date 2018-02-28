# -*- coding: utf-8 -*-
"""
This file defines the global IB connection properties shared among all clients
"""

start_id = 100
_client_id = None


def get_next_id():
    global _client_id
    if not _client_id:
        _client_id = start_id
    else:
        _client_id += 1
    return _client_id

__all__ = ['get_next_id']