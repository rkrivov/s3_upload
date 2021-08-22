#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from string import Template
from typing import Any, Optional, Dict, List, Text, Union, Tuple

from s3.s3base.s3consts import VM_SNAPSHOT_DAYS_COUNT, VM_STATUS_RESET, VM_STATUS_RESTART, VM_STATUS_PAUSE, \
    VM_STATUS_RESUME, VM_STATUS_START, VM_STATUS_STOP, VM_STATUS_SUSPEND
from s3.s3base.s3typing import VirtualMachineID
from s3.s3functions import convert_uuid_to_string, check_result
from s3.s3parallels.errors import VMError, VMUnknownUUIDError
from s3.s3parallels.objects.snapshot import ParallelsSnapshot
from s3.s3parallels.objects.virtualmachine import ParallelsVirtualMachine
from s3.s3parallels.uuid_decoder import UUIDDecoder
from utils import consts
from utils.app_logger import get_logger
from utils.convertors import convert_value_to_type, time_to_string
from utils.exceptions import ExecuteCommandException
from utils.functions import run_command, find_uuid
from utils.metasingleton import MetaSingleton
from utils.notify import notify

logger = get_logger(__name__)


def _json_decode(json_text: Text) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
    try:
        json_data = json.loads(json_text, cls=UUIDDecoder)

        return json_data
    except json.JSONDecodeError:
        raise VMError(json_text) from None


class Parallels(metaclass=MetaSingleton):
    def __init__(self):
        self._vm_list = []

        self._parallels_home_path = os.path.join(consts.HOME_FOLDER, 'Parallels')

        try:
            self._parallels_control_command = run_command('which prlctl')
        except ExecuteCommandException as err:
            logger.error(err)
            self._parallels_control_command = '/usr/local/bin/prlctl'

        self._vm_names_cache = {}

        if self._parallels_control_command == '':
            raise VMError('Command prlctl is not exists.')

    @property
    def parallels_home_path(self) -> str:
        return self._parallels_home_path

    def _notify(self, message: Text, virtual_machine_id: VirtualMachineID):
        vm_name = self.get_virtual_machine_name(virtual_machine_id=virtual_machine_id)
        vm_id_s = convert_uuid_to_string(vm_id=virtual_machine_id).upper()

        notify(message=message,
               title=f'VM {vm_name}' if vm_name != vm_id_s else 'Parallels Virtual Machine',
               subtitle=vm_id_s)

    def _execute_parallels_command(self, command: Union[str, List[str], Tuple[str, str]], /, **kwargs) -> Any:
        if isinstance(command, list) or isinstance(command, tuple):
            command_template_string = ' '.join(command)
        elif isinstance(command, str):
            command_template_string = command
        else:
            raise AttributeError('Command has incorrect argument_type.')

        command_template_string = command_template_string.strip()

        command_template = Template(command_template_string)
        command_string = command_template.substitute(kwargs)

        result = self._execute(self._parallels_control_command, command_string)

        return result

    def check(self, virtual_machine_id: VirtualMachineID) -> bool:
        try:
            vm_info = self.get_virtual_machine(virtual_machine_id=virtual_machine_id)
            vm_id_result = convert_uuid_to_string(vm_info.get('id', uuid.uuid4()), use_curly_brackets=False).upper()
            vm_id_src = convert_uuid_to_string(virtual_machine_id, use_curly_brackets=False).upper()
            if vm_id_src == vm_id_result:
                return True
        except:
            pass
        return False

    def check_snapshot_exist(self, virtual_machine_id: VirtualMachineID, snapshot_id: str) -> bool:
        snapshot_list = self.get_snapshot_list(virtual_machine_id=virtual_machine_id)

        if snapshot_id in snapshot_list:
            return True

        return False

    def create_snapshot(self,
                        virtual_machine_id: VirtualMachineID,
                        name: Optional[Text] = None,
                        description: Optional[Text] = None):

        virtual_machine_id = convert_uuid_to_string(virtual_machine_id)
        virtual_machine_name = self.get_virtual_machine_name(virtual_machine_id=virtual_machine_id)

        currenr_snapshot_date = datetime.now()

        if description is None:
            description = ''

        if name is None:
            name = ''

        name = name.strip()
        description = description.strip()

        if len(name) == 0:
            name = f'Snapshot for {virtual_machine_name} was created on {currenr_snapshot_date.strftime("%Y-%m-%d %H:%M:%S")}'

        if len(description) == 0:
            description = f'This snapshot for {virtual_machine_name} was created on {currenr_snapshot_date.strftime("%Y-%m-%d %H:%M:%S")}'

        logger.info(f'Create snapshot "{name}" for VM {virtual_machine_name} ({virtual_machine_id})...')

        result = self._execute_parallels_command(
            'snapshot $uuid --name "$name" --description "$description"',
            uuid=convert_uuid_to_string(virtual_machine_id),
            name=name,
            description=description
        )

        result = result.split('\n')

        if len(result) > 0:
            result = result[-1]

        result = find_uuid(result)

        if result is not None:
            self._notify(f'A new virtual_machine_snapshot {result} was created.', virtual_machine_id)

        return result

    def delete_snapshot(self, virtual_machine_id: VirtualMachineID, snapshot_id: str, delete_child: bool = False):
        virtual_machine_name = self.get_virtual_machine_name(virtual_machine_id=virtual_machine_id)

        logger.info(
            f'Delete snapshot {snapshot_id} from VM {virtual_machine_name} ({virtual_machine_id})'
        )

        delete_command = ['snapshot-delete', '$uuid', '--id $snapshot_id']

        if delete_child:
            delete_command.append('--children')

        self._execute_parallels_command(delete_command,
                                        uuid=convert_uuid_to_string(virtual_machine_id),
                                        snapshot_id=snapshot_id)

        self._notify(message='Snapshot {!r} was deleted.'.format(snapshot_id), virtual_machine_id=virtual_machine_id)

    def get_spanshot(self, virtual_machine_id: VirtualMachineID, snapshot_id: str) -> Optional[ParallelsSnapshot]:

        for snapshot in self.get_snapshot_iter(virtual_machine_id=virtual_machine_id):
            fetched_snapshot_id = snapshot.get('id', None)
            if fetched_snapshot_id is not None:
                if convert_uuid_to_string(fetched_snapshot_id) == convert_uuid_to_string(snapshot_id):
                    return snapshot
        return None

    def get_snapshot_iter(self, virtual_machine_id: VirtualMachineID) -> ParallelsSnapshot:
        snapshot_list = self.get_snapshot_list(virtual_machine_id=virtual_machine_id)
        for snapshot in snapshot_list:
            yield snapshot

        return None

    def get_snapshot_list(self, virtual_machine_id: VirtualMachineID) -> List[ParallelsSnapshot]:
        snapshot_list = []

        virtual_machine_id = convert_uuid_to_string(virtual_machine_id)

        snapshot_list_text = self._execute_parallels_command(
            ['snapshot-list', '$uuid', '--json'],
            uuid=convert_uuid_to_string(vm_id=virtual_machine_id)
        )

        json_data = _json_decode(snapshot_list_text)

        if len(json_data) > 0:
            for snapshot_id, snapshot in json_data.items():
                parallelsSnapshot = ParallelsSnapshot(dictionary=snapshot)

                parallelsSnapshot.put('days', 0.0)
                parallelsSnapshot.put('id', snapshot_id)

                if not isinstance(parallelsSnapshot.date, datetime):
                    snapshot_date = convert_value_to_type(parallelsSnapshot.date, to_type=datetime)
                    if snapshot_date is not None:
                        parallelsSnapshot.date = snapshot_date

                        p_days = float((datetime.now() - snapshot_date).total_seconds()) / float(consts.SECONDS_PER_DAY)
                        p_days = math.floor(p_days)
                        p_days = int(p_days)

                        parallelsSnapshot.days = p_days

                        # logger.debug(
                        #     f"Snapshot {snapshot_id} expire "
                        #     f"{(snapshot_date + timedelta(days=VM_SNAPSHOT_DAYS_COUNT)).strftime('%Y-%m-%d %H:%M')}"
                        #     f" ({parallelsSnapshot.days} {'days' if parallelsSnapshot.days > 1 else 'day'})"
                        # )

                        logger.info(
                            f"Snapshot {snapshot_id} was created on {parallelsSnapshot.date.strftime('%Y-%m-%d %H:%M')} "
                            f"and expire "
                            f"{(snapshot_date + timedelta(days=VM_SNAPSHOT_DAYS_COUNT)).strftime('%Y-%m-%d %H:%M')}"
                            f" ({parallelsSnapshot.days} {'days' if parallelsSnapshot.days > 1 else 'day'})"
                        )

                snapshot_list.append(parallelsSnapshot)

            snapshot_list.sort(key=lambda virtual_machine_snapshot: virtual_machine_snapshot.get('date', datetime.min))

        return snapshot_list

    def get_virtual_machine_status(self, virtual_machine_id: VirtualMachineID) -> str:
        result = self._execute_parallels_command(('status', '$uuid'), uuid=convert_uuid_to_string(virtual_machine_id))

        result = re.split(r'\s+', result)
        result = result[-1]

        return result.lower()

    def get_virtual_machine(self, virtual_machine_id: VirtualMachineID) -> ParallelsVirtualMachine:
        vm_list_text = self._execute_parallels_command(['list', '$uuid', '--info', '--full', '--json'],
                                                       uuid=convert_uuid_to_string(virtual_machine_id))

        try:
            info_list = _json_decode(vm_list_text)

            if len(info_list) == 0:
                raise VMUnknownUUIDError(virtual_machine_id)

            info = info_list[0]

            virtual_machine = ParallelsVirtualMachine(info)

            return virtual_machine

        except json.JSONDecodeError:
            raise VMUnknownUUIDError(virtual_machine_id)

    def get_virtual_machine_name(self, virtual_machine_id: VirtualMachineID) -> str:
        name = self._vm_names_cache.get(virtual_machine_id)
        if name is None:
            name = convert_uuid_to_string(vm_id=virtual_machine_id, use_curly_brackets=True).upper()
            virtual_machine_info = self.get_virtual_machine(virtual_machine_id=virtual_machine_id)

            if virtual_machine_info is not None:
                value = virtual_machine_info.name

                if value is not None and len(value) > 0:
                    name = value

            self._vm_names_cache[virtual_machine_id] = name

        return name

    def get_virtual_machine_list(self) -> List[uuid.UUID]:
        ret = []

        result = self._execute_parallels_command(['list', '--all', '--output id'])

        lines = result.split('\n')

        if len(lines) > 1:
            lines = lines[1:]

            for line in lines:
                vm_uuid = uuid.UUID(line)
                ret.append(vm_uuid)

        return ret

    def get_virtual_machine_iter(self, virtual_machine_id_list: Optional[
        List[VirtualMachineID]] = None) -> ParallelsVirtualMachine:
        if virtual_machine_id_list is None:
            virtual_machine_id_list = []

        if len(virtual_machine_id_list) < 1:
            virtual_machine_id_list = self.get_virtual_machine_list()

        for vm_uuid in virtual_machine_id_list:
            vm_info = self.get_virtual_machine(virtual_machine_id=vm_uuid)

            if vm_info is not None:
                yield vm_info

        return None

    def set_virtual_machine_status(self, virtual_machine_id: VirtualMachineID, status: str):
        logger.info(f'Set status {status.upper()} to VM {self.get_virtual_machine_name(virtual_machine_id)}')
        self._execute_parallels_command(('$status', '$uuid'),
                                        status=status,
                                        uuid=convert_uuid_to_string(virtual_machine_id))

    # def archive(self, virtual_machine_id: VirtualMachineID):
    #     self._notify('We begin to archive VM...', virtual_machine_id)
    #     try:
    #         self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status='archive')
    #     finally:
    #         self._notify('We finished archiving VM.', virtual_machine_id)

    # def unarchive(self, virtual_machine_id: VirtualMachineID):
    #     self._notify('We begin to unarchive VM...', virtual_machine_id)
    #     try:
    #         self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status='unarchive')
    #     finally:
    #         self._notify('We finished unarchiving VM.', virtual_machine_id)

    def switch_snapshot(self, virtual_machine_id: VirtualMachineID, snapshot_id: str, skip_resume: bool = False):
        switch_snapshot_command = 'snapshot-switch $virtual_machine_id --id $snapshot_id'

        if skip_resume:
            switch_snapshot_command += ' --skip-resume'

        self._execute_parallels_command(switch_snapshot_command, vm_uuid=convert_uuid_to_string(virtual_machine_id),
                                        snapshot_id=snapshot_id)

    def pause_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_PAUSE)
        self._notify('VM was paused.', virtual_machine_id)

    def reset_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_RESET)
        self._notify('VM was reset.', virtual_machine_id)

    def restart_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_RESTART)
        self._notify('VM was restated.', virtual_machine_id)

    def resume_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_RESUME)
        self._notify(f'VM was resumed.', virtual_machine_id)

    def start_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_START)
        self._notify('VM was  started.', virtual_machine_id)

    def stop_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_STOP)
        self._notify('VM was stopped.', virtual_machine_id)

    def suspend_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        self.set_virtual_machine_status(virtual_machine_id=virtual_machine_id, status=VM_STATUS_SUSPEND)
        self._notify('VM was suspended.', virtual_machine_id)

    # def pack(self, virtual_machine_id: VirtualMachineID) -> None:
    #     self._notify('We begin to need_pack VM...', virtual_machine_id)
    #     try:
    #         self._execute_parallels_command('pack $virtual_machine_id',
    #                  vm_uuid=convert_uuid_to_string(virtual_machine_id))
    #     finally:
    #         self._notify('We finished packing VM.', virtual_machine_id)

    # def unpack(self, virtual_machine_id: VirtualMachineID) -> None:
    #     self._notify('We begin to unpack VM...', virtual_machine_id)
    #     try:
    #         self._execute_parallels_command('unpack $virtual_machine_id',
    #                  vm_uuid=convert_uuid_to_string(virtual_machine_id))
    #     finally:
    #         self._notify('We finished unpacking VM.', virtual_machine_id)

    @staticmethod
    def _execute(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            output = run_command(*args, **kwargs)
            check_result(result=output)
            return output
        finally:
            end_time = time.time()
            logger.debug(f'ELAPSED TIME: {time_to_string(end_time - start_time, use_milliseconds=True)}')
