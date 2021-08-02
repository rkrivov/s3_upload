#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from string import Template
from typing import Any, Optional, Dict, List

from common import consts
from common.app_logger import get_logger
from common.convertors import convert_value_to_type, time_to_string
from common.notify import notify
from common.utils import find_uuid, run_command
from s3._base._consts import VM_SNAPSHOT_DAYS_COUNT
from s3._base._typing import VM_UUID
from s3.parallels.errors import VMError, VMUnknownSpanshotError, VMUnknownUUIDError
from s3.parallels.snapshot import ParallelsSnapshot
from s3.utils import convert_uuid_to_string, check_result

logger = get_logger(__name__)


class Parallels(object):
    def __init__(self):
        self._vm_list = []

        self._parallels_path = os.path.join(consts.HOME_FOLDER, 'Parallels')
        self._prlctl = self._execute('which prlctl')

        if self._prlctl == '':
            raise VMError('Command prlctl is not exists.')

    @property
    def path(self) -> str:
        return self._parallels_path

    def _notify(self, message):
        notify(message=message, title='s3_upload', subtitle=type(self).__name__);

    def run(self, command: str, /, **kwargs) -> Any:
        command_templ = Template(command)
        command_str = command_templ.substitute(kwargs)

        result = self._execute(self._prlctl, command_str)

        return result

    def check(self, vm_id: VM_UUID) -> bool:
        try:
            vm_info = self.get_vm_info(vm_id=vm_id)
            vm_id_result = convert_uuid_to_string(vm_info.get('id', uuid.uuid4()), use_curly_brackets=False).upper()
            vm_id_src = convert_uuid_to_string(vm_id, use_curly_brackets=False).upper()
            if vm_id_src == vm_id_result:
                return True
        except:
            pass
        return False

    def check_snapshot_exist(self, vm_id: VM_UUID, snapid: str) -> bool:
        snapshot_list = self.get_snapshot_list(vm_id=vm_id)

        if snapid in snapshot_list:
            return True

        return False

    def create_snapshot(self, vm_id: VM_UUID, name: Optional[str] = None, description: Optional[str] = None):
        vm_id = convert_uuid_to_string(vm_id)

        snapshot_date = datetime.now()
        vm_name = self.get_vm_name(vm_id=vm_id)

        if description is None:
            description = ''

        if name is None:
            name = ''

        name = name.strip()
        description = description.strip()

        if len(name) == 0:
            name = f'Snapshot for {snapshot_date.strftime("%Y-%m-%d %H:%M:%S")}'

        if len(description) == 0:
            description = f'Snapshot for VM {vm_name} was created on {snapshot_date.strftime("%Y-%m-%d %H:%M:%S")}'

        logger.info(f'Create snapshot "{name}" for VM {vm_name} ({vm_id})...')

        result = self.run('snapshot $vm_uuid --name "$name" --description "$description"',
                          vm_uuid=convert_uuid_to_string(vm_id),
                          name=name,
                          description=description)

        result = result.split('\n')

        if len(result) > 0:
            result = result[-1]

        result = find_uuid(result)

        return result

    def delete_snapshot(self, vm_id: VM_UUID, snap_id: str, delete_child: bool = False):
        spanshot_list = self.get_snapshot_list(vm_id=vm_id)
        vm_name = self.get_vm_name(vm_id=vm_id)

        logger.info(f'Delete snapshot {snap_id} from VM {vm_name} ({vm_id})')

        if snap_id not in spanshot_list:
            raise VMUnknownSpanshotError(vm_id, snap_id)

        if delete_child:
            delete_command = 'snapshot-delete $vm_uuid --id $snapshot_id --children'
        else:
            delete_command = 'snapshot-delete $vm_uuid --id $snapshot_id'

        self.run(delete_command,
                 vm_uuid=convert_uuid_to_string(vm_id),
                 snapshot_id=snap_id)

    def get_spanshot(self, vm_id: VM_UUID, snap_id: str) -> Optional[ParallelsSnapshot]:
        snapshot_list = self.get_snapshot_list(vm_id=vm_id)
        return snapshot_list.get(snap_id, None)

    def get_snapshot_list(self, vm_id: VM_UUID) -> Dict[str, ParallelsSnapshot]:
        vm_id = convert_uuid_to_string(vm_id)

        json_text = self.run('snapshot-list ${vm_id} --json',
                             vm_id=convert_uuid_to_string(vm_id=vm_id))
        json_data = json.loads(json_text)

        snapshot_list = {}

        for snapid, snapshot in json_data.items():
            parallelsSnapshot = ParallelsSnapshot(dictionary=snapshot)

            parallelsSnapshot.put('days', 0.0)
            parallelsSnapshot.put('id', snapid)

            if not isinstance(parallelsSnapshot.date, datetime):
                snapshot_date = convert_value_to_type(parallelsSnapshot.date, to_type=datetime)
                if snapshot_date is not None:
                    parallelsSnapshot.date = snapshot_date
                    logger.debug(
                        f"Snapshot {snapid} expire "
                        f"{(snapshot_date + timedelta(days=VM_SNAPSHOT_DAYS_COUNT)).strftime('%Y-%m-%d')}"
                    )

                    p_days = float((datetime.now() - snapshot_date).total_seconds()) / float(consts.SECONDS_PER_DAY)
                    p_days = math.floor(p_days)
                    p_days = int(p_days)

                    parallelsSnapshot.days = p_days

            snapshot_list[snapid] = parallelsSnapshot

        return snapshot_list

    def get_status(self, vm_id: VM_UUID) -> str:
        result = self.run('status $vm_uuid', vm_uuid=convert_uuid_to_string(vm_id))

        result = re.split(r'\s+', result)
        result = result[-1]

        return result.lower()

    def get_vm_info(self, vm_id: VM_UUID) -> Dict[str, Any]:
        info_text = self.run("list --info --full --json $vm_uuid",
                             vm_uuid=convert_uuid_to_string(vm_id))

        try:
            info_list = json.loads(info_text)

            if len(info_list) == 0:
                raise VMUnknownUUIDError(vm_id)

            info = info_list[0]

            vm_id = info.get('ID', None)
            vm_name = info.get('Name', None)
            vm_description = info.get('Description', None)
            vm_type = info.get('Type', None)
            vm_state = info.get('State', None)
            vm_os = info.get('OS', None)
            vm_uptime = info.get('Uptime', None)
            vm_config = info.get('Home path', None)
            vm_home = info.get('Home', None)

            if len(vm_uptime) > 0 and vm_uptime.isdigit():
                vm_uptime = int(vm_uptime)
            else:
                vm_uptime = 0

            return {
                'id': uuid.UUID(vm_id),
                'name': vm_name,
                'description': vm_description,
                'type': vm_type,
                'state': vm_state,
                'os': vm_os,
                'uptime': vm_uptime,
                'config': vm_config,
                'home': vm_home
            }

        except json.JSONDecodeError:
            raise VMUnknownUUIDError(vm_id)

    def get_vm_name(self, vm_id: VM_UUID) -> str:
        info = self.get_vm_info(vm_id=vm_id)

        if info is not None:
            name = info.get('name', None)
            if name is not None and len(name) > 0:
                return name

        raise VMUnknownUUIDError(vm_id)

    def get_vm_list(self) -> List[uuid.UUID]:
        ret = []

        result = self.run('list --all --output id')

        lines = result.split('\n')

        if len(lines) > 1:
            lines = lines[1:]

            for line in lines:
                vm_uuid = uuid.UUID(line)
                ret.append(vm_uuid)

        return ret

    def set_status(self, vm_id: VM_UUID, status: str):
        logger.info(f'Set status {status.upper()} to VM {self.get_vm_name(vm_id)}')
        self._notify(f'We set status to {status.upper()} for VM {convert_uuid_to_string(vm_id)}')
        self.run('$status $vm_uuid',
                 status=status,
                 vm_uuid=convert_uuid_to_string(vm_id))

    def pause(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='pause')

    def reset(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='reset')

    def restart(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='restart')

    def resume(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='resume')

    def switch_snapshot(self, vm_id: VM_UUID, snapshot_id: str, skip_resume: bool = False):
        switch_snapshot_command = 'snapshot-switch $vm_uuid --id $snapshot_id'

        if skip_resume:
            switch_snapshot_command += ' --skip-resume'

        self.run(switch_snapshot_command, vm_uuid=convert_uuid_to_string(vm_id), snapshot_id=snapshot_id)

    def start(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='start')

    def stop(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='stop')

    def suspend(self, vm_id: VM_UUID):
        self.set_status(vm_id=vm_id, status='suspend')

    def pack_vm(self, vm_id: VM_UUID) -> None:
        self.run('pack $vm_uuid',
                 vm_uuid=convert_uuid_to_string(vm_id))

    def unpack_vm(self, vm_id: VM_UUID) -> None:
        self.run('unpack $vm_uuid',
                 vm_uuid=convert_uuid_to_string(vm_id))

    @staticmethod
    def _execute(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            command_line = ' '.join(args)

            if isinstance(command_line, bytes):
                command_line = command_line.decode(json.detect_encoding(command_line))

            logger.debug(f'EXECUTE COMMAND: {command_line}')

            output = run_command(*args, **kwargs)

            check_result(result=output)

            return output
        finally:
            end_time = time.time()
            logger.debug(f'ELAPSED TIME: {time_to_string(end_time - start_time, use_milliseconds=True)}')
