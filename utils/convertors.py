#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import base64
import json
import math
import os
import re
from datetime import datetime
from string import Template
from typing import Union, Type, Optional, Any

from utils import consts
from utils.consts import RUS_TO_LAT


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


def size_from_human(human_size: str) -> Union[float, int]:
    one_suffixes = 'KMGTPEZY'
    two_suffixes = ('KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    iec_suffixes = ('KIB', 'MIB', 'GIB', 'TIB', 'PIB', 'EIB', 'ZIB', 'YIB')

    ret_value = 0

    pattern = r'((?P<number>\d+(?:[.\,]\d+)?)\s*(?P<suffix>[kKmMgGtTpPeEzZyY]?(?:[Ii])?[Bb])(/[Ss])?)'

    result = re.match(pattern, human_size)

    if result is not None:
        result_dict = result.groupdict()
        if result_dict is not None:
            number = result_dict.get('number', None)
            suffix = result_dict.get('suffix', None)

            depth = degree_base = suffix_index = None

            if number is not None:
                result_dict = re.match(r'([\s\,\d]+)([,\.])(\d+)', number.strip())
                if result_dict is not None:
                    items = result_dict.groups()
                    number = ''
                    for item in items:
                        if item not in ['.', ',']:
                            item = [char for char in item if char.isdigit()]
                        number += item
                number = re.sub(r'(\d+)([,\.])(\d+)', r'\1.\3', number)

                ret_value = float(number)

                if suffix is not None:
                    suffix = suffix.strip()
                    suffix = suffix.upper()

                    if len(suffix) > 0:
                        if suffix in iec_suffixes:
                            suffix_index = iec_suffixes.index(suffix)
                            depth = 10
                            degree_base = 2
                        elif suffix in one_suffixes or suffix in two_suffixes:
                            suffix_index = iec_suffixes.index(suffix)
                            depth = 3
                            degree_base = 10
                        else:
                            raise ValueError(f'Incorrect suffix ({suffix})')

                        degree_value = (suffix_index + 1) * depth
                        multiplier = math.pow(degree_base, degree_value)
                        ret_value = ret_value * multiplier

    return ret_value


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
                        new_value = int(size_from_human(value))
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
                elif 'T' in value:
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
                    time_formats = ('%S', '%M', '%H')
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


def convert_string_to_statement(text: Union[bytes, str]) -> str:
    text = text.lower().strip()
    text = rus_to_lat(text)
    return re.sub(r"\W+", "_", text)


def convert_value_to_string(value: Any) -> str:
    """
    Convert any argument_type value to string
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
    mseconds = int((time_value - float(seconds)) * 1000)

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
        template_values[name.upper()] = value

    for name, value in template_values.items():
        if value in ret:
            index = ret.index(value)
            length = len(value)
            ret = ret[:index] + '${' + name.upper() + '}' + ret[index + length:]

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
        template_values[name.upper()] = value

    # ret = re.sub(r"(%\w+%)", lambda m: template_values.get(m.group(0).upper()), ret, flags=re.IGNORECASE)
    pattern = r'(' + re.escape('${') + r'\w+' + re.escape('}') + ')'

    ret = re.sub(pattern, lambda m: m.group(0).upper(), ret, flags=re.IGNORECASE)

    template_string = Template(ret)
    ret = template_string.safe_substitute(template_values)

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
    days = hours // 24
    hours = hours % 24

    if days > 0:
        if days == 1:
            ret = 'one day'
        else:
            ret = f'{days} days'

        f = hours / 24.0

        if f > 0.0:
            if f >= 0.5:
                if (days + 1) > 1:
                    ret = f'about {int(days + 1)} days'
                else:
                    ret = 'about one day'
            else:
                ret = f'over {ret}'
    elif hours > 0:
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
