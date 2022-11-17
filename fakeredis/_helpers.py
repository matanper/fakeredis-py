import logging
import re
from typing import List, Tuple, Dict

LOGGER = logging.getLogger('fakeredis')
REDIS_LOG_LEVELS = {
    b'LOG_DEBUG': 0,
    b'LOG_VERBOSE': 1,
    b'LOG_NOTICE': 2,
    b'LOG_WARNING': 3
}
REDIS_LOG_LEVELS_TO_LOGGING = {
    0: logging.DEBUG,
    1: logging.INFO,
    2: logging.INFO,
    3: logging.WARNING
}

MAX_STRING_SIZE = 512 * 1024 * 1024


class SimpleString:
    def __init__(self, value):
        assert isinstance(value, bytes)
        self.value = value

    @classmethod
    def decode(cls, value):
        return value


class NoResponse:
    """Returned by pub/sub commands to indicate that no response should be returned"""
    pass


OK = SimpleString(b'OK')
QUEUED = SimpleString(b'QUEUED')
PONG = SimpleString(b'PONG')
BGSAVE_STARTED = SimpleString(b'Background saving started')


def null_terminate(s):
    # Redis uses C functions on some strings, which means they stop at the
    # first NULL.
    if b'\0' in s:
        return s[:s.find(b'\0')]
    return s


def casenorm(s):
    return null_terminate(s).lower()


def casematch(a, b):
    return casenorm(a) == casenorm(b)

# todo:
# def parse_args(args: List[bytes], allowed_args: Tuple[str, ...]) -> Tuple[Tuple[bool, ...], List[bytes]]:
#     """Parse items in args, and for each allowed args,
#     whether it is present or not and a list of remaining args
#     >>> parse_args([b'nx', b'xx', b'gt', b'tt'], ('xx', 'nx', 'zz'))
#     ((True, True, False), [b'gt', b'tt'])
#
#     """
#     present_dict: Dict[str, bool] = {casenorm(arg.encode()): False for arg in allowed_args}
#     remaining_args = list()
#     for arg in args:
#         if casenorm(arg) in present_dict:
#             present_dict[casenorm(arg)] = True
#         else:
#             remaining_args.append(arg)
#     return tuple(present_dict[casenorm(arg.encode())] for arg in allowed_args), remaining_args


def compile_pattern(pattern):
    """Compile a glob pattern (e.g. for keys) to a bytes regex.

    fnmatch.fnmatchcase doesn't work for this, because it uses different
    escaping rules to redis, uses ! instead of ^ to negate a character set,
    and handles invalid cases (such as a [ without a ]) differently. This
    implementation was written by studying the redis implementation.
    """
    # It's easier to work with text than bytes, because indexing bytes
    # doesn't behave the same in Python 3. Latin-1 will round-trip safely.
    pattern = pattern.decode('latin-1', )
    parts = ['^']
    i = 0
    pattern_len = len(pattern)
    while i < pattern_len:
        c = pattern[i]
        i += 1
        if c == '?':
            parts.append('.')
        elif c == '*':
            parts.append('.*')
        elif c == '\\':
            if i == pattern_len:
                i -= 1
            parts.append(re.escape(pattern[i]))
            i += 1
        elif c == '[':
            parts.append('[')
            if i < pattern_len and pattern[i] == '^':
                i += 1
                parts.append('^')
            parts_len = len(parts)  # To detect if anything was added
            while i < pattern_len:
                if pattern[i] == '\\' and i + 1 < pattern_len:
                    i += 1
                    parts.append(re.escape(pattern[i]))
                elif pattern[i] == ']':
                    i += 1
                    break
                elif i + 2 < pattern_len and pattern[i + 1] == '-':
                    start = pattern[i]
                    end = pattern[i + 2]
                    if start > end:
                        start, end = end, start
                    parts.append(re.escape(start) + '-' + re.escape(end))
                    i += 2
                else:
                    parts.append(re.escape(pattern[i]))
                i += 1
            if len(parts) == parts_len:
                if parts[-1] == '[':
                    # Empty group - will never match
                    parts[-1] = '(?:$.)'
                else:
                    # Negated empty group - matches any character
                    assert parts[-1] == '^'
                    parts.pop()
                    parts[-1] = '.'
            else:
                parts.append(']')
        else:
            parts.append(re.escape(c))
    parts.append('\\Z')
    regex = ''.join(parts).encode('latin-1')
    return re.compile(regex, re.S)

# todo: is this needed?
# class _DummyParser:
#     def __init__(self, socket_read_size):
#         self.socket_read_size = socket_read_size
#
#     def on_disconnect(self):
#         pass
#
#     def on_connect(self, connection):
#         pass
