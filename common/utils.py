#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import base64
import errno
import hashlib
import subprocess
from hashlib import md5
import json
import math
import os
import random
import re
import shutil
import tempfile
import threading
import time
import uuid
from datetime import datetime
from typing import Any, AnyStr, Callable, List, Optional, Type, Tuple, Union

from common import consts
from common import filewrapper
from common import utils
from common.consts import RUS_TO_LAT
from common.filewrapper import FileWrapper
from s3.object import S3Object

make_tmp_file_name = lambda: tempfile.mktemp(suffix="-" + str(uuid.uuid4().hex), dir=consts.TEMP_FOLDER)


def append_end_separator(path: str, sep: str = os.path.sep):
    if not path.endswith(sep):
        path = path + sep
    return path


def append_start_separator(path: str, sep: str = os.path.sep):
    if not path.startswith(sep):
        path = sep + path
    return path


def remove_end_separator(path: str, sep: str = os.path.sep):
    if path.endswith(sep):
        path = path[:-1]
    return path


def remove_start_separator(path: str, sep: str = os.path.sep):
    if path.startswith(sep):
        path = path[1:]
    return path


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


def size_to_human(size: Union[int, float], use_iec: bool = False) -> str:
    if use_iec:
        suffixes = ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB')
        depth = 10
        log_method = math.log2
        degree_base = 2
    else:
        suffixes = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
        depth = 3
        log_method = math.log10
        degree_base = 10

    degree_value = log_method(size) if size > 0 else 0
    suffix_index = int(math.floor(degree_value / depth))
    degree_value = suffix_index * depth

    if suffix_index >= len(suffixes):
        suffix_index = len(suffixes) - 1

    divisor = math.pow(degree_base, degree_value)

    return f'{int(size)} {suffixes[suffix_index]}' if suffix_index == 0 \
        else f'{float(size) / divisor:.2f} {suffixes[suffix_index]}'


def inscribe_message(message: Union[str, bytes], width: Optional[int] = None) -> str:
    if width is None:
        width = get_terminal_width()

    if isinstance(message, bytes):
        message = message.decode(consts.ENCODER)

    if len(message) > (width - 3):
        left_part_len = width // 2
        right_part_pos = (len(message) - left_part_len) +3
        left_part = message[:left_part_len]
        right_part = message[right_part_pos:]
        message = f'{left_part}...{right_part}'

    return message


def run_command(*args, **kwargs) -> Any:
    try:
        shell = kwargs.pop('shell', True)

        stdout = kwargs.pop('stdout', subprocess.PIPE)
        stderr = kwargs.pop('stderr', subprocess.STDOUT)

        input = kwargs.pop('input', None)
        timeout = kwargs.pop('timeout', None)

        command = ' '.join(args)

        process = subprocess.Popen(command, shell=shell, stdout=stdout, stderr=stderr)

        output = process.communicate(input=input, timeout=timeout)

        if len(output) > 0:
            output = output[0]
            output = output.decode(json.detect_encoding(output))
            output = output.strip()
        else:
            output = None

        return output
    except:
        pass

    return None

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


def convert_value_to_string(value: Any) -> str:
    """
    Convert any type value to string
    """
    result = ''

    if value is not None:
        if isinstance(value, list):
            items = []

            for item in value:
                items.append(convert_value_to_string(item))

            result = ', '.join(items)
            result = f"[{result}]"
        elif isinstance(value, tuple):
            items = []

            for item in value:
                items.append(convert_value_to_string(item))

            result = ', '.join(items)
            result = f"({result})"
        elif isinstance(value, dict):
            items = []

            for name, item in value.items():
                items.append(f"{name} = {convert_value_to_string(item)}")

            result = ', '.join(items)
            result = f"{{{result}}}"
        elif isinstance(value, datetime):
            result = convert_value_to_string(value.isoformat())
        elif isinstance(value, str):
            result = f"\"{value}\""
        else:
            result = f'{value}'

    return result


def get_time_from_string(time_in_string: str) -> float:
    result = 0.0

    pos = time_in_string.rindex('.') != -1

    if pos != -1:
        milliseconds = time_in_string[pos + 1:] if pos < (len(time_in_string) - 1) else '0'
        time_in_string = time_in_string[:pos - 1] if pos > 0 else ''
        result = float(f'0.{milliseconds}')

    if ':' in time_in_string:
        time_parts = time_in_string[0].split(':')

        seconds = 0

        for time_part in time_parts:
            if not time_part.isdigit():
                raise ValueError(f"'{time_part}' (in '{time_in_string}') ism't numerical.")
            seconds = seconds * 60 + int(time_part)
        result += float(seconds)
    else:
        if not time_in_string.isdigit():
            raise ValueError(f'{result} is not numerical.')
        result += int(time_in_string)

    return result


def convert_value_to_type(value: Any, to_type: Type[Any], default: Optional[Any] = None) -> Any:
    if type(value) == to_type or to_type is None:
        return value

    new_value = default

    if to_type == str:
        new_value = str(value)
    elif to_type == bool:
        if isinstance(value, str):
            value = value.strip()

            if not value:
                new_value = False
            else:
                if value.lower().strip() in ['1', 'true', 'yes', 'y', 'ok', 'on']:
                    new_value = True
                elif value.lower().strip() in ['0', 'false', 'no', 'n', 'nok', 'off']:
                    new_value = False
                else:
                    raise ValueError(
                        "Value {value} could not be converted to {type}".format(value=value, type=to_type.__name__))
        elif isinstance(value, (int, float)):
            if int(value) == 0:
                new_value = False
            else:
                new_value = True
        else:
            raise ValueError(
                "Value {value} could not be converted to {type}".format(value=value, type=to_type.__name__))
    elif to_type == int:
        try:
            if not value:
                new_value = 0
            else:
                if isinstance(value, str):
                    value = value.strip()

                    if re.match(r"^((\d{2}\:)?(\d{2}\:)?\d{2})$", value) is not None:
                        new_value = int(get_time_from_string(value))
                    elif re.match(
                            r'((?P<number>\d+(?:[.\,]\d+)?)\s*(?P<suffix>[kKmMgGtTpPeEzZyY]?(?:[Ii])?[Bb])(/[Ss])?)',
                            value) is not None:
                        new_value = int(size_to_human(value))
                    else:
                        new_value = int(value)
                elif isinstance(value, datetime):
                    new_value = int(value.timestamp())
                else:
                    new_value = int(value)
        except ValueError:
            raise ValueError(
                "Value {value} could not be converted to {type}".format(value=value, type=to_type.__name__)) from None
    elif to_type == float:
        try:
            if not value:
                new_value = 0.0
            else:
                if isinstance(value, str):
                    value = value.strip()

                    if re.match(r"^((\d{2}\:)?(\d{2}\:)?\d{2})$", value) is not None:
                        new_value = get_time_from_string(value)
                    elif re.match(
                            r'((?P<number>\d+(?:[\.\,]\d+)?)\s*(?P<suffix>[kKmMgGtTpPeEzZyY]?(?:[Ii])?[Bb])(/[Ss])?)',
                            value) is not None:
                        new_value = size_to_human(value)
                    else:
                        new_value = float(value)
                elif isinstance(value, datetime):
                    new_value = value.timestamp()
                else:
                    new_value = float(value)
        except ValueError:
            raise ValueError(
                "Value {value} could not be converted to {type}".format(value=value, type=to_type.__name__)) from None
    elif to_type is datetime:
        if isinstance(value, str):
            value = value.strip()

            date_separators: str = '.-/'
            date_value: str = value
            time_value: str = ''
            time_separator: str = ''
            date_separator: str = ''

            has_date: bool = False
            has_time: bool = False

            date_format: str = ''
            time_format: str = ''

            if ' ' in value or 'T' in value:
                has_date = True
                has_time = True

                if ' ' in value:
                    time_separator = ' '
                elif 'T'in value:
                    time_separator = 'T'

                date_value, time_value = value.split(time_separator)
            elif ':' in value:
                date_value = ''
                time_value = value
                has_date = False
                has_time = True
            else:
                date_value = value
                time_value = ''
                has_date = True
                has_time = False

            if has_date:
                for sep in date_separators:
                    if sep in date_value:
                        date_separator = sep
                        break
                else:
                    raise ValueError(f"'{date_value}' has incorrent format.")

            if has_time:
                has_timezone: bool = False
                has_milliseconds: bool = False
                has_pmam: bool = False

                if time_value.lower().endswith('am') or time_value.lower().endswith('pm'):
                    has_pmam = True
                    time_value = time_value[-2]

                try:
                    plus_pos = time_value.rindex('+')
                except ValueError:
                    plus_pos = -1

                if plus_pos != -1:
                    has_timezone = True
                    time_value = time_value[:plus_pos - 1]

                try:
                    dot_pos = time_value.rindex('.')
                except:
                    dot_pos = -1

                if plus_pos != -1:
                    has_milliseconds = True
                    time_value = time_value[:dot_pos - 1]

                if ':' in time_value:
                    time_formats = ('%S', '%M' ,'%H')
                    time_format_parts = []

                    for ix in range(len(time_value.split(':'))):
                        time_format_parts.append(time_formats[ix])
                    time_format_parts.reverse()
                    time_format = ':'.join(time_format_parts)
                else:
                    time_format = '%S'

                if has_milliseconds:
                    time_format += '%f'

                if has_timezone:
                    time_format += '%z'

                if has_pmam:
                    time_format += '%p'

            if has_date:
                date_formats = (
                    f"%Y{date_separator}%m{date_separator}%d",
                    f"%Y{date_separator}%d{date_separator}%m",
                    f"%m{date_separator}%d{date_separator}%Y",
                    f"%d{date_separator}%m{date_separator}%Y"
                )

                for date_format in date_formats:
                    format = date_format

                    if has_time:
                        format += time_separator
                        format += time_format

                    try:
                        new_value = datetime.strptime(value, format)
                        break
                    except ValueError:
                        new_value = default
            else:
                try:
                    value = datetime.strptime(value, time_format)
                except ValueError:
                    new_value = default

            # datetime_fmts = [
            #     "%Y-%m-%d",
            #     "%Y-%d-%m",
            #     "%d-%m-%Y",
            #     "%m-%d-%Y",
            #
            #     "%Y-%m-%dT%H:%M:%S",
            #     "%Y-%d-%mT%H:%M:%S",
            #     "%d-%m-%YT%H:%M:%S",
            #     "%m-%d-%YT%H:%M:%S",
            #
            #     "%Y-%m-%dT%H:%M:%S%Z",
            #     "%Y-%d-%mT%H:%M:%S%Z",
            #     "%d-%m-%YT%H:%M:%S%Z",
            #     "%m-%d-%YT%H:%M:%S%Z",
            #
            #     "%Y-%m-%dT%H:%M:%S%z",
            #     "%Y-%d-%mT%H:%M:%S%z",
            #     "%d-%m-%YT%H:%M:%S%z",
            #     "%m-%d-%YT%H:%M:%S%z",
            #
            #     "%Y-%m-%dT%H:%M:%S.%f",
            #     "%Y-%d-%mT%H:%M:%S.%f",
            #     "%d-%m-%YT%H:%M:%S.%f",
            #     "%m-%d-%YT%H:%M:%S.%f",
            #
            #     "%Y-%m-%dT%H:%M:%S.%f%z",
            #     "%Y-%d-%mT%H:%M:%S.%f%z",
            #     "%d-%m-%YT%H:%M:%S.%f%z",
            #     "%m-%d-%YT%H:%M:%S.%f%z",
            #
            #     # ---
            #     "%Y-%m-%d %H:%M:%S",
            #     "%Y-%d-%m %H:%M:%S",
            #     "%d-%m-%Y %H:%M:%S",
            #     "%m-%d-%Y %H:%M:%S",
            #
            #     "%Y-%m-%d %H:%M:%S%Z",
            #     "%Y-%d-%m %H:%M:%S%Z",
            #     "%d-%m-%Y %H:%M:%S%Z",
            #     "%m-%d-%Y %H:%M:%S%Z",
            #
            #     "%Y-%m-%d %H:%M:%S%z",
            #     "%Y-%d-%m %H:%M:%S%z",
            #     "%d-%m-%Y %H:%M:%S%z",
            #     "%m-%d-%Y %H:%M:%S%z",
            #
            #     "%Y-%m-%d %H:%M:%S.%f",
            #     "%Y-%d-%m %H:%M:%S.%f",
            #     "%d-%m-%Y %H:%M:%S.%f",
            #     "%m-%d-%Y %H:%M:%S.%f",
            #
            #     "%Y-%m-%d %H:%M:%S.%f%z",
            #     "%Y-%d-%m %H:%M:%S.%f%z",
            #     "%d-%m-%Y %H:%M:%S.%f%z",
            #     "%m-%d-%Y %H:%M:%S.%f%z",
            #     # ---
            #
            #     "%d %b %Y %H:%M:%S",
            #     "%a, %d %b %Y %H:%M:%S",
            #     "%a, %d %b %Y %H:%M:%S %Z",
            #
            #     "%d %b %Y %H:%M:%S",
            #     "%d %b %Y %H:%M:%S %Z",
            #     "%d %b %Y %H:%M:%S%p",
            #     "%d %b %Y %H:%M%p",
            #
            #     "%b %d %Y %I:%M%p"
            # ]
            #
            # last_datetime_fmt_idx = len(datetime_fmts) - 1
            #
            # for datetime_fmt_idx, datetime_fmt in enumerate(datetime_fmts):
            #     try:
            #         new_value = datetime.strptime(value, datetime_fmt)
            #         break
            #     except ValueError:
            #         if datetime_fmt_idx == last_datetime_fmt_idx:
            #             raise ValueError(f"Value {value} could not be converted to {to_type.__name__}") from None

        elif isinstance(value, (float, int)):
            new_value = datetime.fromtimestamp(float(value))
        else:
            raise ValueError(f"Value {value} could not be converted to {to_type.__name__}")
    else:
        raise ValueError(f"Value {value} could not be converted to {to_type.__name__}")

    return new_value


def convert_arguments_to_string(*args, **kwargs):
    """
    Convert arguments from *args and **kwargs to string
    """

    ret = []

    if len(args) > 0:
        for arg in args:
            ret.append(convert_value_to_string(arg))

    if len(kwargs) > 0:
        for name, value in kwargs.items():
            ret.append(f'{name} = {convert_value_to_string(value)}')

    return ', '.join(ret)


def rus_to_lat(rus_str: Union[bytes, str]) -> str:
    upper_chars = 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'
    ret = ''

    for char in rus_str:
        need_upper = char in upper_chars
        char = char.lower()
        if char in RUS_TO_LAT:
            char = RUS_TO_LAT[char]

        if need_upper:
            if len(char) > 1:
                char = char[0].upper() + char[1:].lower()
            else:
                char = char.upper()

        ret += char

    return ret


def convert_string_to_statement(text: Union[bytes, str]) -> str:
    text = text.lower().strip()
    text = rus_to_lat(text)
    return re.sub(r"\W+", "_", text)


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


def speed_to_mbps(speed: Union[int, float]) -> str:
    suffixes = ('bps', 'kbps', 'mbps', 'gbps', 'tbps', 'pbps', 'ebps', 'zbps', 'ybps')

    if speed == 0:
        return '0 bps'

    degree_value = math.log10(speed)
    suffix_index = int(math.floor(degree_value / 3.0))

    if suffix_index >= len(suffixes):
        suffix_index = len(suffixes) - 1

    speed_mpbs = (float(speed) / math.pow(10, 3 * suffix_index)) / 0.125

    return '{speed:.2f} {suffix}'.format(speed=speed_mpbs, suffix=suffixes[suffix_index])


def time_to_string(time_value: Union[int, float], use_milliseconds: bool = False, human: bool = False) -> str:
    seconds = int(time_value)
    mseconds = int((time_value - float(seconds))  * 1000)

    hours = seconds // consts.SECONDS_PER_HOUR
    minutes = (seconds % consts.SECONDS_PER_HOUR) // consts.SECONDS_PER_MINUTE
    seconds = (seconds % consts.SECONDS_PER_HOUR) % consts.SECONDS_PER_MINUTE
    days = hours // 24
    hours = hours % 24

    if human:
        time_parts = []

        if days > 0:
            time_parts.append(f'{days} day{"s" if days > 1 else ""}')
        if hours > 0:
            time_parts.append(f'{hours} hour{"s" if hours > 1 else ""}')
        if minutes > 0:
            time_parts.append(f'{minutes} minute{"s" if minutes > 1 else ""}')
        if seconds > 0:
            time_parts.append(f'{seconds} second{"s" if seconds > 1 else ""}')
        if use_milliseconds and mseconds > 0:
            time_parts.append(f'{mseconds} millisecond{"s" if mseconds > 1 else ""}')

        if len(time_parts) > 0:
            ret = ' '.join(time_parts)
        else:
            ret = '0 seconds'
    else:
        if hours > 0:
            if use_milliseconds:
                ret = f'{hours:02d}:{minutes:02d}:{seconds:02d}.{mseconds:03d}'
            else:
                ret = f'{hours:02d}:{minutes:02d}:{seconds:02d}'
        elif minutes > 0:
            if use_milliseconds:
                ret = f'{minutes:02d}:{seconds:02d}.{mseconds:03d}'
            else:
                ret = f'{minutes:02d}:{seconds:02d}'
        elif seconds > 0:
            if use_milliseconds:
                ret = f'{seconds:02d}.{mseconds:03d} {get_string_case(seconds, "second", "seconds")}'
            else:
                ret = f'{seconds:02d} {get_string_case(seconds, "second", "seconds")}'
        else:
            ret = f'{mseconds:03d} {get_string_case(seconds, "millisecond", "milliseconds")}'

        if days > 0:
            ret = f'{days} d. {ret}'

    return ret


def make_template_from_string(*args, **kwargs) -> str:
    ret = ''.join(args)

    template_values = {
        # 'CONTAINER': remove_end_path_sep(consts.CONTAINERS_FOLDER),
        # 'LIB': remove_end_path_sep(consts.LIB_FOLDER),
        # 'HOME': remove_end_path_sep(consts.HOME_FOLDER),
        # 'WORK': remove_end_path_sep(consts.WORK_FOLDER),
        # 'TEMP': remove_end_path_sep(consts.TEMP_FOLDER)
    }

    for name, value in kwargs.items():
        template_values[name] = value

    for name, value in template_values.items():
        if value in ret:
            index = ret.index(value)
            length = len(value)
            ret = ret[:index] + '%' + name + '%' + ret[index + length:]

    return ret


def make_string_from_template(*args, **kwargs) -> str:
    ret = ''.join(args)
    template_values = {
        'CONTAINER': remove_end_path_sep(consts.CONTAINERS_FOLDER),
        'LIB': remove_end_path_sep(consts.LIB_FOLDER),
        'HOME': remove_end_path_sep(consts.HOME_FOLDER),
        'WORK': remove_end_path_sep(consts.WORK_FOLDER),
        'TEMP': remove_end_path_sep(consts.TEMP_FOLDER),
        'CURRENT_DATE': datetime.now().strftime('%Y%m%d'),
        'CURRENT_TIME': datetime.now().strftime('%H%M%S'),
    }

    for name, value in kwargs.items():
        name = '%' + name.upper() + '%'
        template_values[name] = value

    ret = re.sub(r"(%\w+%)", lambda m: template_values.get(m.group(0).upper()), ret, flags=re.IGNORECASE)

    return ret


def mstime_to_string(time_value: int, use_milliseconds: bool = False) -> str:
    ret = ''

    seconds = time_value // consts.MSECONDS_PER_SECOND
    time_value = time_value % consts.MSECONDS_PER_SECOND

    hours = seconds // consts.SECONDS_PER_HOUR
    minutes = (seconds % consts.SECONDS_PER_HOUR) // consts.SECONDS_PER_MINUTE
    seconds = (seconds % consts.SECONDS_PER_HOUR) % consts.SECONDS_PER_MINUTE

    if hours > 0:
        ret = f'{hours:02d}:{minutes:02d}:{seconds:02d}.{time_value:03d}'
    elif minutes > 0:
        ret = f'{minutes:02d}:{seconds:02d}.{time_value:03d}'
    elif seconds > 0:
        ret = f'{seconds:02d}.{time_value:03d} {get_string_case(seconds, "second", "seconds")}'
    elif use_milliseconds:
        ret = f'{time_value} ms.'
    return ret


def time_to_short_string(time_value: Union[int, float]) -> str:
    ret = ''

    seconds = int(time_value)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 3600) % 60

    if hours > 0:
        if hours == 1:
            ret = 'one hour'
        else:
            ret = f'{hours} hours'

        f = (float(minutes) + float(seconds) / 60.0) / 60.0

        if f > 0.0:
            if f >= 0.5:
                if (hours + 1) > 1:
                    ret = f'about {int(hours + 1)} hours'
                else:
                    ret = 'about one hour'
            else:
                ret = f'over {ret}'
    elif minutes > 0:
        if minutes == 0:
            ret = 'one minute'
        else:
            ret = f'{minutes} minutes'

        f = float(seconds) / 60.0

        if f > 0:
            if f >= 0.5:
                if minutes < 59:
                    if (minutes + 1) > 1:
                        ret = f'about {int(minutes + 1)} minutes'
                    else:
                        ret = f'about one minute'
                else:
                    ret = f'about one hour'

            else:
                ret = f'over {ret}'
    elif seconds > 0:
        if seconds == 1:
            ret = 'one seconds'
        else:
            ret = f'{seconds} seconds'

    return ret


def get_string_case(value: Union[int, float], string_case_1: Union[str, bytes], string_case_2: Union[str, bytes],
                    string_case_3: Optional[Union[str, bytes]] = None) -> str:
    if string_case_3 is None:
        string_case_3 = string_case_2

    frac_part = 0.0

    value = abs(value)

    if isinstance(value, float):
        frac_part = value - int(value)
        value = int(value)

    if value > 99:
        value = value % 100

    if value > 19:
        value = value % 10

    if value == 0 or value > 4:
        return string_case_3
    elif 1 < value < 5:
        return string_case_2

    if frac_part > 0.00:
        return string_case_2

    return string_case_1


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


def append_end_path_sep(file_path: Union[str, bytes]) -> str:
    if len(file_path) > 0 and not file_path.endswith(os.path.sep):
        file_path = file_path + os.path.sep
    return file_path


def remove_end_path_sep(file_path: Union[str, bytes]) -> str:
    if len(file_path) > 0 and file_path.endswith(os.path.sep):
        file_path = file_path[:-1]
    return file_path


def append_start_path_sep(file_path: Union[str, bytes]) -> str:
    if len(file_path) > 0 and not file_path.startswith(os.path.sep):
        file_path = os.path.sep + file_path
    return file_path


def remove_start_path_sep(file_path: Union[str, bytes]) -> str:
    if len(file_path) > 0 and file_path.startswith(os.path.sep):
        file_path = file_path[1:]
    return file_path


def pad(text: Union[bytes, str], key: Union[bytes, str]) -> Union[bytes, str]:
    key_len = len(key)

    while (len(text) % key_len) != 0:
        text += key

    return text


def encode_string(value: str) -> str:
    encoded_value = base64.b64encode(value.encode(consts.ENCODER)).decode(consts.ENCODER)
    return encoded_value


def encode(value: Any) -> str:
    if not isinstance(value, str):
        if isinstance(value, dict):
            value = json.dumps(value, indent=4)
        else:
            value = convert_value_to_string(value)
    return encode_string(value)


def decode_string(encoded_value: str) -> str:
    value = base64.b64decode(encoded_value.encode(consts.ENCODER)).decode(consts.ENCODER)
    return value


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
            return result

    return None

def get_file_size(file_name) -> int:
    if isinstance(file_name, str):
        try:
            return os.stat(file_name).st_size
        except Exception:
            return 0
    else:
        return filewrapper.total_len(file_name)


def _get_file_name(o) -> Optional[str]:
    if o is not None:
        if not isinstance(o, str):
            if hasattr(o, 'fd'):
                fd = o.fd
                if hasattr(fd, 'name'):
                    return fd.name
                if hasattr(fd, '__str__'):
                    return str(fd)
            if hasattr(o, '__str__'):
                return str(o)
    return o


def _pack_file_name(filename: str) -> str:
    for name, path in consts.FOLDERS.items():
        if filename.startswith(path):
            filename = filename[len(path):]
            filename = append_start_path_sep(file_path=filename)
            return f'{{{name}}}{filename}'

        return filename


def _calc_hash(file_object,
               length: Union[int, float],
               hash_name: AnyStr = consts.MD5_ENCODER_NAME,
               as_base64: bool = False,
               chuck_size: int = consts.BUFFER_SIZE,
               show_progress: bool = True) -> str:
    if not hasattr(file_object, 'read'):
        raise Exception(f"The file object <{type(file_object).__name__}> is unreadable.")

    if not filewrapper.is_callable(file_object.read):
        raise Exception(f"The file object <{type(file_object).__name__}> is unreadable.")

    if not hasattr(file_object, 'close'):
        raise Exception(f"The file object <{type(file_object).__name__}> doesn't close.")

    if not filewrapper.is_callable(file_object.close):
        raise Exception(f"The file object <{type(file_object).__name__}> doesn't close.")

    o_hash = hashlib.new(name=hash_name)

    fname = _get_file_name(file_object)

    if show_progress:
        progress = ProgressBar(caption=f'Calculate hash {hash_name.upper()}', max_value=length)
    try:
        chuck_size = min(length, chuck_size)

        while file_object.left_bytes > 0:
            buffer = file_object.read(chuck_size)

            if not buffer:
                raise IOError(errno.EIO, os.strerror(errno.EIO), fname)

            o_hash.update(buffer)

            if show_progress:
                progress.value += len(buffer)
    finally:
        if show_progress:
            del progress

    result = o_hash.hexdigest()
    return encode_string(result) if as_base64 else result


def _calc_file_hash(file_object: Union[str, bytes, int],
                    hash_name: AnyStr = consts.MD5_ENCODER_NAME,
                    as_base64: bool = False,
                    chuck_size: int = consts.BUFFER_SIZE,
                    show_progress: bool = True) -> str:
    file_wrapper = FileWrapper.create(file=file_object, mode="rb", chuck_size=consts.BUFFER_SIZE,
                                      encoding=consts.ENCODER)

    return _calc_hash(file_object=file_wrapper, length=get_file_size(file_wrapper), hash_name=hash_name,
                      as_base64=as_base64, chuck_size=chuck_size,
                      show_progress=show_progress)


def calc_file_hash(file_object: Union[str, bytes, int, S3Object],
                   hash_name: AnyStr = consts.MD5_ENCODER_NAME,
                   as_base64: bool = False,
                   chuck_size: int = consts.BUFFER_SIZE,
                   show_progress: bool = True) -> str:
    if isinstance(file_object, S3Object):
        return _calc_hash(
            file_object=file_object,
            length=len(file_object),
            hash_name=hash_name,
            as_base64=as_base64,
            chuck_size=chuck_size,
            show_progress=show_progress
        )
    else:
        return _calc_file_hash(
            file_object,
            hash_name=hash_name,
            as_base64=as_base64,
            chuck_size=chuck_size,
            show_progress=show_progress
        )


def get_file_etag(file_name: Union[str, bytes, int], show_progress: bool = True) -> str:
    if isinstance(file_name, bytes):
        file_name = file_name.decode(json.detect_encoding(file_name))

    file_size = get_file_size(file_name=file_name)
    part_size  = min(file_size, consts.BUFFER_SIZE)

    if file_size > consts.FILE_SIZE_LIMIT:
        num_of_parts = (file_size // consts.BUFFER_SIZE)
        if (file_size % consts.BUFFER_SIZE) != 0:
            num_of_parts += 1
    else:
        num_of_parts = 1

    # md5_hash = hashlib.new('md5')
    md5_digests = []

    if file_size > 0:
        if show_progress:
            progress = ProgressBar(caption=f'Calculate ETag...', max_value=file_size)

        try:
            with open(file_name, 'rb') as f:
                for chunk in iter(lambda: f.read(part_size), b''):
                    if show_progress:
                        progress.value = progress.value + len(chunk)

                    md5_digests.append(md5(chunk).digest())

                    # if num_of_parts > 1:
                    #     md5_digests.append(md5(chunk).digest())
                    # else:
                    #     md5_hash.update(chunk)


        finally:
            if show_progress:
                del progress

    etag = md5(b''.join(md5_digests)).hexdigest()

    if num_of_parts > 1:
        etag = etag  + '-' + str(num_of_parts)

    return etag


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


class Arguments(object):

    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs
        self.__current_index = 0
        self.__arguments_count = len(self.__args)

    def get(self, argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None, throw_error: bool = True) -> Any:
        if self.__current_index < self.__arguments_count:
            result = get_parameter(
                self.__args,
                self.__current_index,
                argument_type=argument_type,
                throw_error=throw_error)

            if result is not None:
                self.__current_index += 1

            return result
        return None

    def get_all_for(self, argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None):
        ret = None
        index = self.__current_index
        while index < self.__arguments_count:
            if argument_type is not None:
                if isinstance(self.__args[index], argument_type):
                    if ret is None:
                        ret = []
                    ret.append(self.__args[index])
            else:
                if ret is None:
                    ret = []
                ret.append(self.__args[index])
            index += 1

        if ret is not None:
            ret = tuple(ret)

        return ret

    def pop(self, key: AnyStr, default: Any = None) -> Any:
        return self.__kwargs.pop(key, default=default)

    def __str__(self):
        return f"{self.__args}"


class Closer(object):
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj

    def __exit__(self, exception_type, exception_val, trace):
        if self.obj is not None:
            try:
                self.obj.close()
            except AttributeError:
                pass

        return True


class ProgressBar(object):
    def __init__(self, caption: Union[str, bytes],
                 max_value: Union[int, float],
                 min_value: Optional[Union[int, float]] = None,
                 show_size_in_bytes: Optional[bool] = True):
        self._caption = caption
        self._min_value = min_value if min_value is not None else 0
        self._max_value = max_value
        self._cur_value = self._min_value
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._speed = 0
        self._elapsed = 0

        self._show_size_in_bytes = show_size_in_bytes

        self._display()

    def __del__(self):
        self.hide()

    def _build_progress_bar(self, bar_width: Union[int, float]) -> str:
        if self.percent == 0.0:
            progress_bar = ' ' * (bar_width - 2)
        elif self.percent >= 1.0:
            progress_bar = '=' * (bar_width - 2)
        else:
            progress_bar = ''

            fill_width = int(float(bar_width - 2) * float(self.percent))
            empty_width = (bar_width - 2) - fill_width

            progress_bar += '=' * (fill_width - 1)
            progress_bar += ' ' * empty_width

        return f'[{progress_bar}]'

    def _build_progress_info(self) -> str:
        end_time = time.time()
        difference = end_time - self._start_time

        if hasattr(self, '_old_difference'):
            old_difference = self._old_difference
        else:
            old_difference = 0

        if hasattr(self, '_old_cur_value'):
            old_cur_value = self._old_cur_value
        else:
            old_cur_value = 0

        seen_so_far = (self._cur_value - old_cur_value) - self._min_value
        left_bytes = self.max_value - self._cur_value

        if difference >= 5:
            self._old_cur_value = self._cur_value
            self._speed = seen_so_far / difference
            self._elapsed = left_bytes / self._speed if self._speed != 0 else 0
            self._start_time = end_time
            self._old_difference = old_difference + difference

        progress_bar_info_parts = []

        if self._show_size_in_bytes:
            progress_bar_info_parts.append(f'{self.percent:.02%} / {size_to_human(self._max_value)}')
        else:
            progress_bar_info_parts.append(f'{self.percent:.02%} / {self._max_value}')

        if 0.025 < self.percent <= 1.0:
            elapsed_time_string = time_to_short_string(self._elapsed)
            if len(elapsed_time_string) > 0:
                progress_bar_info_parts.append(f'{time_to_short_string(self._elapsed)} left')

        if self._speed > 0:
            if self._show_size_in_bytes:
                progress_bar_info_parts.append(f'{speed_to_mbps(self._speed)}')
            else:
                progress_bar_info_parts.append(f'{self._speed:.02f} per seconds')


        progress_bar_info = '  '.join(progress_bar_info_parts)

        if len(progress_bar_info) > 0:
            progress_bar_info = '  ' + progress_bar_info

        return progress_bar_info

    def _make_message(self) -> str:
        terminal_width = utils.get_terminal_width() - 2
        column_width = terminal_width // 4
        progress_bar = []

        progress_bar.append(utils.inscribe_message(f'{self._caption}  ', width=column_width))
        progress_bar.append(self._build_progress_bar(bar_width=column_width))
        progress_bar.append(utils.inscribe_message(self._build_progress_info(), width=terminal_width - column_width * 2))

        return ''.join(progress_bar)

    def _display(self) -> None:
        message = self._make_message()
        with self._lock:
            show_message('\r' + message + consts.CLEAR_TO_END_LINE)

    def __call__(self, *args, **kwargs) -> None:
        bytes_amount = get_parameter(args, 0, (int, float))
        self._cur_value = self._cur_value + bytes_amount
        self._cur_value = max(self._min_value, self._cur_value)
        self._cur_value = min(self._max_value, self._cur_value)
        self._display()

    @property
    def max_value(self) -> Union[int, float]:
        return self._max_value

    @max_value.setter
    def max_value(self, value: Union[int, float]):
        self._max_value = value
        self._cur_value = min(value, self._cur_value)

    @property
    def min_value(self) -> Union[int, float]:
        return self._min_value

    @min_value.setter
    def min_value(self, value: Union[int, float]):
        self._min_value = value
        self._cur_value = max(value, self._cur_value)

    @property
    def percent(self) -> float:
        return (float(self._cur_value) / float(self._max_value)) if self._max_value != 0.00 else 0.00

    @property
    def value(self):
        return self._cur_value

    @value.setter
    def value(self, value: Union[int, float]) -> None:
        self._cur_value = max(self._min_value, min(self._max_value, value))
        self._display()

    def show(self):
        self._display()

    def update(self):
        self._display()

    def hide(self):
        show_message('\r' + consts.CLEAR_TO_END_LINE, end='\r')

class ProgressPercentage(ProgressBar):
    def __init__(self, filename):
        size = os.path.getsize(filename)
        super().__init__(filename, max_value=size)


class ProgressBackupDatabase(ProgressBar):
    def __init__(self, caption: Union[str, bytes]):
        super(ProgressBackupDatabase, self).__init__(caption=caption, max_value=100)

    def __call__(self, *args, **kwargs):
        value = get_parameter(args, 0, (int, float))
        min_value = get_parameter(args, 1, (int, float))
        max_value = get_parameter(args, 2, (int, float))

        self.min_value = min_value
        self.max_value = max_value
        self.value = value
        self._display()


if not os.path.exists(consts.TEMP_FOLDER):
    os.mkdir(consts.TEMP_FOLDER)
