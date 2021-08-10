#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import re
from typing import Optional, Union, Tuple, List, Text
from uuid import UUID

from botocore.exceptions import ClientError

from s3.s3base.s3typing import VirtualMachineID
from s3.s3parallels.errors import VMError
from utils.app_logger import get_logger

logger = get_logger(__name__)


def client_exception_handler(skip_codes: Optional[Union[Tuple[str], List[str]]] = None):
    def wrapper_exception_handler(fn):
        def wrapper_func(*args, **kwargs):
            try:
                result = fn(*args, **kwargs)

                return result
            except ClientError as clientError:
                logger.debug(f"Exception: {clientError}")
                if skip_codes is None:
                    raise
                error = clientError.response.get('Error', {})
                error_code = error.get('Code', 'Unknown')
                if error_code not in skip_codes:
                    raise clientError from None
            return None

        return wrapper_func

    return wrapper_exception_handler


def check_result(result: str):
    logger.debug(f'RESULT: {result}')

    if '\n' in result:
        lines = result.split('\n')
        if len(lines) > 0:
            result = lines[-1]

    result = result.strip()

    logger.debug(f'RESULT MESSAGE: {result}')

    if ('error' in result.lower()) or ('fail' in result.lower()):
        raise VMError(result)


def remote_brackets(value: Text, brackets: Union[str, Tuple[str], List[str]]) -> str:
    def __get_brackets(v: Union[str, Tuple[str], List[str]], ix: int) -> str:
        if isinstance(v, str):
            return v

        if isinstance(v, list) or isinstance(v, tuple):
            if ix >= len(v):
                raise IndexError(f'Index {ix} out of range (0..{len(v) - 1}.')
            return v[ix]

        raise ValueError(f"Incorrect brackets ({v}).")

    start_bracket = __get_brackets(brackets, 0)
    start_bracket = re.escape(start_bracket)

    end_bracket = __get_brackets(brackets, 1)
    end_bracket = re.escape(end_bracket)

    pattern = r'^' + start_bracket + r'(?P<value>[^' + end_bracket + r']+)' + end_bracket + r'$'
    m = re.match(pattern=pattern, string=value)

    if m is not None:
        result = m.groupdict()

        return result.get('value', None) \
            if result is not None \
            else value

    return value


def remove_curly_brackets(value: str) -> str:
    return remote_brackets(value, ('{', '}'))


def convert_uuid_to_string(vm_id: VirtualMachineID, use_curly_brackets: bool = True) -> str:
    if vm_id is None:
        raise ValueError("Virtual Machine Identifier could not be None.")

    if not isinstance(vm_id, str):
        if isinstance(vm_id, bytes):
            vm_id_s = vm_id.decode(json.detect_encoding(vm_id))
        elif isinstance(vm_id, UUID):
            vm_id_s = str(vm_id)
        else:
            raise ValueError(f'Incorrect UUID type ("{type(vm_id).__name__}").')
    else:
        vm_id_s = vm_id

    vm_id_s = remove_curly_brackets(vm_id_s)
    vm_id_s = vm_id_s.strip()

    if use_curly_brackets:
        vm_id_s = f"{{{vm_id_s}}}"

    return vm_id_s.lower()


def convert_uuid_from_string(vm_id: VirtualMachineID) -> UUID:
    if not isinstance(vm_id, UUID):
        if isinstance(vm_id, str):
            ret = UUID(hex=remove_curly_brackets(vm_id))
        elif isinstance(vm_id, bytes):
            ret = UUID(bytes=vm_id)
    else:
        ret = vm_id

    return ret
