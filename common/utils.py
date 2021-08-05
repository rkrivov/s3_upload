#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import contextlib
import io
import json
import math
import os
import random
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from typing import Any, Callable, List, Optional, Tuple, Union, Dict, AnyStr

from common import consts
from common.consts import ENCODER, CLEAR_TO_END_LINE
from common.exceptions import ExecuteCommandException

make_tmp_file_name = lambda: tempfile.mktemp(suffix="-" + str(uuid.uuid4().hex), dir=consts.TEMP_FOLDER)


@contextlib.contextmanager
def reset(buffer):
    original_position = buffer.tell()
    buffer.seek(0, 2)
    yield buffer.seek(original_position, 0)


def is_callable(obj):
    if hasattr(obj, '__call__'):
        return True

    return callable(obj)




def encode_with(buffer: AnyStr, encoding: AnyStr) -> Any:
    if buffer is not None:

        if isinstance(buffer, memoryview):
            return buffer

        if isinstance(buffer, io.BytesIO):
            return buffer

        if not isinstance(buffer, bytes):
            return buffer.encode(encoding)

    return buffer


def read_data_from(data: Union[bytes, memoryview, io.IOBase], length: int = -1) -> Any:
    if hasattr(data, 'read'):
        return data.read(length)
    return data


def total_len(o):
    if hasattr(o, '__len__'):
        return len(o)

    if hasattr(o, 'len'):
        return o.len

    if hasattr(o, 'headers'):
        value = o.headers.get('content-length', None)
        if value is not None and value.isdigit():
            return int(value)

    if hasattr(o, 'fileno'):
        try:
            fileno = o.fileno()
        except io.UnsupportedOperation:
            pass
        else:
            return os.fstat(fileno).st_size

    if hasattr(o, 'getvalue'):
        # e.g. BytesIO, cStringIO.StringIO
        return len(o.getvalue())


def get_current_position(fobj: Any) -> int:
    if fobj:
        if hasattr(fobj, "tell"):
            if is_callable(fobj.tell):
                return calling_method(fobj.tell)
    return 0


def next_chunk(buffer: Union[bytes, memoryview, io.IOBase], chunk_size: int) -> Optional[Union[bytes, memoryview]]:
    current_position = get_current_position(fobj=buffer)
    total_size = total_len(buffer)

    if chunk_size <= 0:
        chunk_size = 2 << 20

    if total_size > current_position:
        chunk_size = min(chunk_size, total_size - current_position)

        buffer = read_data_from(buffer, chunk_size)

        if buffer:
            return memoryview(buffer)

    return None


def to_list(fields: Union[Tuple[Any], List[Any], Dict[Any, AnyStr]]) -> List[Any]:
    if hasattr(fields, 'items'):
        return list(fields.items())
    return list(fields)


def is_closed(obj: Any) -> bool:
    if obj:
        if hasattr(obj, "closed"):
            if is_callable(obj.closed):
                return obj.closed()
            else:
                return obj.closed
    return True


def close(obj: Any) -> None:
    if obj:
        if hasattr(obj, "close"):
            if is_callable(obj.close):
                obj.close()



def calling_method(callable_object, *args, **kwargs):
    if is_callable(callable_object):
        return callable_object(*args, **kwargs)

    return None


def is_equal(value1: Any, value2: Any) -> bool:
    """
    Equal two values: value1 and value2.
    Return true if value1 is equal to value2. Otherwise, it returns false.
    """
    if value1 is not None and value2 is not None:
        if type(value1) == type(value2):
            return value1 == value2
        else:
            return False
    else:
        if value1 is None and value2 is not None:
            return False
        elif value1 is not None and value2 is None:
            return False
        return True


def inscribe_message(message: Union[str, bytes], width: Optional[int] = None) -> str:
    if width is None:
        width = get_terminal_width()

    if isinstance(message, bytes):
        message = message.decode(consts.ENCODER)

    if len(message) > (width - 3):
        left_part_len = width // 2
        right_part_pos = (len(message) - left_part_len) + 3
        left_part = message[:left_part_len]
        right_part = message[right_part_pos:]
        message = f'{left_part}...{right_part}'

    return message


def run_command(*args, **kwargs) -> Any:

    shell = kwargs.pop('shell', True)

    stdout = kwargs.pop('stdout', subprocess.PIPE)
    stderr = kwargs.pop('stderr', subprocess.STDOUT)

    input = kwargs.pop('input', None)
    timeout = kwargs.pop('timeout', None)

    command = ' '.join(args)

    process = subprocess.Popen(command, shell=shell, stdout=stdout, stderr=stderr)

    output = process.communicate(input=input, timeout=timeout)
    return_code = process.returncode

    if len(output) > 0:
        output = output[0]
        output = output.decode(json.detect_encoding(output))
        output = output.strip()
    else:
        output = ''

    if return_code != 0:
        error_message = f"{output}"

        if len(error_message) == 0:
            error_message = f'Command failed with code: {return_code} [{command}]'
        else:
            error_message = f"{error_message} (retcode is {return_code})."

        raise ExecuteCommandException(error_message) from None

    return output


def get_string(o) -> str:
    if isinstance(o, str):
        ret = o
    else:
        if isinstance(o, bytes):
            ret = o.decode(json.detect_encoding(o))
        elif hasattr(o, '__str__'):
            ret = str(o)
        else:
            ret = None

    return ret


def get_terminal_width() -> int:
    terminal_width: int = 0
    fallback: Tuple[int, int] = (80, 25)

    term = os.environ.get('TERM', None)
    if term is not None:
        cols: str = run_command('tput', f'-T{term}', 'cols')
        if cols is not None:
            cols = cols.strip()
            if cols.isdigit():
                terminal_width = int(cols)
    else:
        fallback = (200, 100)

    if terminal_width == 0:
        terminal_width, _ = shutil.get_terminal_size(fallback=fallback)

    return terminal_width


def show_error(message: Union[str, bytes], end: str = '') -> None:
    if isinstance(message, bytes):
        message = message.decode(consts.ENCODER)
    print(str(message), end=end)
    # sys.stderr.write(message + end)
    # sys.stderr.flush()


def show_message(message: Union[str, bytes], end: str = '') -> None:
    if isinstance(message, bytes):
        message = message.decode(consts.ENCODER)
    print(str(message), end=end)
    # sys.stdout.write(message + end)
    # sys.stdout.flush()


def retry(retry_on_exception: Callable[[Exception], bool] = None,
          wait_random_min: int = 1000,
          wait_random_max: int = 5000,
          stop_max_attempt_number: int = 10,
          logger=None):
    def retry_decorate_func(fn):
        def wrapper(*args, **kwargs):
            attempt_number = 0

            while True:
                try:
                    result = fn(*args, **kwargs)
                    return result
                except Exception as exception:
                    if retry_on_exception is not None:
                        if retry_on_exception(exception):
                            attempt_number += 1
                            if attempt_number >= stop_max_attempt_number:
                                raise exception from None
                            wait_time_out = random.randint(int(wait_random_min * math.pow(1.25, attempt_number - 1)),
                                                           int(wait_random_max * math.pow(1.25, attempt_number - 1)))
                            if logger is not None:
                                logger.debug(
                                    f"Exception {type(exception).__name__} with message: "
                                    f"{exception}. Attempt number {attempt_number + 1}. "
                                    f"Pause {wait_time_out / consts.MSECONDS_PER_SECOND} s."
                                )
                            time.sleep(float(wait_time_out) / float(consts.MSECONDS_PER_SECOND))
                        else:
                            raise
                    else:
                        raise

        return wrapper

    return retry_decorate_func


def find_uuid(value: str) -> Optional[str]:
    uuid_reg_patterns = (
        r'[0-9a-f]{8}\-[0-9a-f]{4}\-[1-4][0-9a-f]{3}\-[89AB][0-9a-f]{3}\-[0-9a-f]{12}',
        r'[0-9a-f]{8}-[0-9a-f]{4}-[5][0-9a-f]{3}-[89AB][0-9a-f]{3}-[0-9a-f]{12}',
        r'[0-9a-f]{8}\\-([0-9a-f]{4}\\-){3}[0-9a-f]{12}',
        r'[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'
    )

    for uuid_reg_pattern in uuid_reg_patterns:
        result = re.search(uuid_reg_pattern, value, re.IGNORECASE)
        if result is not None:
            result = result.regs
            if result is not None and len(result) > 0:
                result = result[0]
                start_pos, end_pos = result
                if start_pos >= 0 and start_pos < len(value) and end_pos >= 0 and end_pos < len(value):
                    result = value[start_pos:end_pos]
                    return f"{{{result}}}"
    return None


def get_parameter(args: Union[Tuple[Any], List[Any]], index: int,
                  argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None, throw_error: bool = True) -> Any:
    if len(args) <= index:
        if throw_error:
            raise ValueError(f"Missing {index} required argument.")
        else:
            return None

    if argument_type is not None:
        if not isinstance(args[index], argument_type):
            if throw_error:
                if isinstance(argument_type, (tuple, list)):
                    types_list = ""
                    for i in range(len(argument_type)):
                        if i == len(argument_type) - 1:
                            if len(types_list) > 0:
                                types_list += " or "
                        else:
                            if len(types_list) > 0:
                                types_list += ", "
                            types_list += argument_type[i].__name__
                else:
                    types_list = argument_type.__name__
                raise TypeError(f"{index} argument has incorrect type. The type must be {types_list}.")
            else:
                return None

    return args[index]


# Print iterations progress
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent_f = iteration / float(total) if total != 0 else 0
    percent = ("{0:." + str(decimals) + "%}").format(percent_f)

    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent} {suffix}{CLEAR_TO_END_LINE}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


if not os.path.exists(consts.TEMP_FOLDER):
    os.mkdir(consts.TEMP_FOLDER)
