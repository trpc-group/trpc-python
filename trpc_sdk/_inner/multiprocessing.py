"""
    trpc multiprocessing
"""
import multiprocessing

PROCESS = multiprocessing.Process


def set_default_process(process_cls):
    global PROCESS
    PROCESS = process_cls
