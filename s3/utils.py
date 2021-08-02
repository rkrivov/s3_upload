#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import re
from typing import Optional, Union, Tuple, List
from uuid import UUID

from botocore.exceptions import ClientError

from common import app_logger
from s3._base._typing import VM_UUID
from s3.parallels.errors import VMError

logger = app_logger.get_logger(__name__)


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
        result = result.split('\n')
        if len(result) > 0:
            result = result[-1]

    result = result.strip()
    if ('error' in result.lower()) or ('fail' in result.lower()):
        raise VMError(result)


def remove_curly_brackets(value: str) -> str:
    items = re.findall(r'\{([^\}]+)\}', value)

    if len(items) > 0:
        return items[-1]

    return value


def convert_uuid_to_string(vm_id: VM_UUID, use_curly_brackets: bool = True) -> str:
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


def convert_uuid_from_string(vm_id: VM_UUID) -> UUID:
    if not isinstance(vm_id, UUID):
        if isinstance(vm_id, str):
            ret = UUID(hex=remove_curly_brackets(vm_id))
        elif isinstance(vm_id, bytes):
            ret = UUID(bytes=vm_id)
    else:
        ret = vm_id

    return ret
