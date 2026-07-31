"""
    declare for TrpcContext
"""
from trpc.trpc_abc.context import Context


class TrpcContext(Context):
    pass


META_DATA_KEY_LOG_FIELDS = "__log_fields__"
