#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import Any


class VMError(Exception):
    pass


class VMIncorrectUUIDError(VMError):
    def __init__(self, vm_id: Any):
        super(VMIncorrectUUIDError, self).__init__(f"Incorrect VM identifier {vm_id}.")


class VMUnknownUUIDError(VMError):
    def __init__(self, vm_id: Any):
        super(VMUnknownUUIDError, self).__init__(f"Unknown VM with identifier {vm_id}.")


class VMUnknownStatusError(VMError):
    def __init__(self, vm_id: Any, status: str):
        super(VMUnknownStatusError, self).__init__(f"VM with identifier {vm_id} has unknown status {status}.")


class VMUnknownSpanshotError(VMError):
    def __init__(self, vm_id: Any, span_id: str):
        super(VMUnknownSpanshotError, self).__init__(f"Snapshot {span_id} for VM {vm_id} isn't exist.")
