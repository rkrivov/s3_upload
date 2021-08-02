#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from string import Template
from typing import Any, List, Dict, Optional, TypeVar
from uuid import UUID

import pytz

from common import app_logger, consts, dbase
from common import utils
from common.dbase import SQLBuilder
from s3.base import S3Base, OP_BACKUP, OP_RESTORE, _get_name_hash, _get_file_info, INFO_FIELD_NAME, INFO_NEW, \
    OP_INSERT, INFO_OP, INFO_FIELD_SIZE, INFO_FIELD_MTIME, INFO_FIELD_HASH, INFO_OLD, OP_DELETE

VM_UUID = TypeVar('VM_UUID', bytes, str, UUID)

VM_STATUS_RUNNING = 'running'
VM_STATUS_PAUSED = 'paused'
VM_STATUS_STOPPED = 'stopped'
VM_STATUS_SUSUPENDED = 'suspended'

VM_SNAPSHOT_POWER_ON = 'poweron'
VM_SNAPSHOT_POWER_OFF = 'poweroff'
VM_SNAPSHOT_SUSPEND = 'suspend'

VM_TYPE_NORMAL = 'PVM'
VM_TYPE_ARCHIVED = 'PVMZ'
VM_TYPE_PACKED = 'PVMP'

VM_SNAPSHOT_COUNT = 3
VM_SNAPSHOT_DAYS_COUNT = 5

logger = app_logger.get_logger(__name__)


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


def check_result(result: str):
    logger.debug(f'RESULT: {result}')

    if '\n' in result:
        result = result.split('\n')
        if len(result) > 0:
            result = result[-1]

    result = result.strip()
    if ('error' in result.lower()) or ('fail' in result.lower()):
        raise VMError(result)


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return convert_uuid_to_string(obj, use_curly_brackets=True)
        return json.JSONEncoder.default(self, obj)


class UUIDDecoder(json.JSONDecoder):
    pass

class ParallelsSnapshot(object):
    def __init__(self, dictionary: Optional[Dict[str, Any]] = None):
        if dictionary is not None:
            for key, value in dictionary.items():
                self.__dict__[key] = value

    def __getitem__(self, item):
        return self.get(item)

    def __iter__(self):
        return self.__dict__.items()

    def __len__(self):
        return len(self.__dict__)

    def __setitem__(self, key, value):
        self.put(key, value)

    def get(self, key: str, default=None) -> Any:
        if key in self.__dict__:
            return self.__dict__[key]

        if default is None:
            raise AttributeError(f'{key} could not be found in {type(self).__name__}.')

        return default

    def put(self, key: str, value: Any) -> None:
        self.__dict__[key] = value


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

        result = utils.find_uuid(result)

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
                snapshot_date = utils.convert_value_to_type(parallelsSnapshot.date, to_type=datetime)
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
                'id': UUID(vm_id),
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

    def get_vm_list(self) -> List[UUID]:
        ret = []

        result = self.run('list --all --output id')

        lines = result.split('\n')

        if len(lines) > 1:
            lines = lines[1:]

            for line in lines:
                vm_uuid = UUID(line)
                ret.append(vm_uuid)

        return ret

    def set_status(self, vm_id: VM_UUID, status: str):
        logger.info(f'Set status {status.upper()} to VM {self.get_vm_name(vm_id)}')
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

            output = utils.run_command(*args, **kwargs)

            check_result(result=output)

            return output
        finally:
            end_time = time.time()
            logger.debug(f'ELAPSED TIME: {utils.time_to_string(end_time - start_time, use_milliseconds=True)}')


class S3ParallelsOperation(S3Base):
    DATABASE = os.path.join(consts.WORK_FOLDER, 'parallels.vm.cache')
    TABLE = "virtual_machines"

    FIELD_ID = 'id'
    FIELD_UUID = 'uuid'
    FIELD_CONFIG = 'config'

    SCRIPT = f"""
    --- The scipt was written by Roman N. Krivov a.k.a. Eochaid Bres Drow
    --- on {datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S %Z')} 

    --- Create table {TABLE} if it doesn't exists
    CREATE TABLE IF NOT EXISTS {TABLE} (
        {FIELD_ID} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        {FIELD_UUID} TEXT NOT NULL,
        {FIELD_CONFIG} TEXT);

    --- Create indexes for table {TABLE}
    CREATE UNIQUE INDEX IF NOT EXISTS {TABLE}_{FIELD_UUID}_idx ON {TABLE} ({FIELD_UUID});
    """

    def __init__(self, bucket: str):
        self._database = dbase.create_database(name=S3ParallelsOperation.DATABASE)
        self._database.execute_script(S3ParallelsOperation.SCRIPT)

        self._parallels = None
        self._suspended = {}
        self._packed = {}
        self._vm_uuid = None

        super(S3ParallelsOperation, self).__init__(local_path=self.parallels.path, bucket=bucket,
                                                   remote_path='Parallels/')

    def __del__(self):
        if hasattr(self, '_database'):
            if self._database is not None:
                del self._database
        super(S3ParallelsOperation, self).__del__()

    @property
    def parallels(self) -> Parallels:
        parallels = None

        if hasattr(self, '_parallels'):
            parallels = self._parallels

        if parallels is None:
            parallels = Parallels()
            self._parallels = parallels

        return parallels

    @property
    def vm_uuid(self) -> UUID:
        ret = None

        if hasattr(self, '_vm_uuid'):
            ret = self._vm_uuid

        return ret

    @vm_uuid.setter
    def vm_uuid(self, value: UUID):
        self._vm_uuid = value

    def _get_vm_info(self, vm_id: VM_UUID) -> Optional[Dict[str, Any]]:
        vm_info = self._load_vm_info(vm_uuid=vm_id)

        if vm_info is None:
            try:
                vm_info = self.parallels.get_vm_info(vm_id=vm_id)
            except:
                vm_info = None

        return vm_info

    def _fetch_local_files_list(self, local_path: str = None,
                                operations: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        if local_path is None:
            local_path = self._local_path

        if operations is None:
            operations = {}

        if os.path.isfile(local_path):
            local_file_name = local_path
            local_path = os.path.dirname(local_file_name)

            name = local_file_name

            if name.startswith(local_path):
                name = name[len(local_path):]
                name = utils.append_start_path_sep(name)

            name_hash = _get_name_hash(name)

            file_info = _get_file_info(file=local_file_name)

            operation_info = operations.setdefault(name_hash, {})
            operation_info[INFO_NEW] = file_info
            operation_info[INFO_OP] = OP_INSERT
        else:
            for root, dirs, files in os.walk(self._local_path):
                for file in files:
                    if not file.startswith('.') and not file.startswith('~'):
                        local_file_path = os.path.join(
                            utils.append_end_path_sep(root),
                            utils.remove_start_path_sep(file)
                        )

                        name = local_file_path
                        if name.startswith(local_path):
                            name = name[len(local_path):]
                            name = utils.append_start_path_sep(name)

                        name_hash = _get_name_hash(name)

                        file_info = _get_file_info(file=local_file_path)

                        operation_info = operations.setdefault(name_hash, {})
                        operation_info[INFO_NEW] = file_info
                        operation_info[INFO_OP] = OP_INSERT

        return operations

    def _fetch_remote_files_list(self, remote_path: str = None,
                                 operations: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        if remote_path is None:
            remote_path = self._archive_path

        if operations is None:
            operations = {}

        prefix = utils.append_end_path_sep(remote_path)
        objects_count = self.storage.get_objects_count(prefix=prefix)

        if objects_count > 0:
            progress = None

            if self.show_progress:
                progress = utils.ProgressBar('Update connection',
                                             max_value=objects_count,
                                             show_size_in_bytes=False)

            try:
                for remote_object in self.storage.fetch_bucket_objects(prefix=prefix):
                    if progress is not None:
                        progress.value = progress.value + 1

                    file_name = remote_object.get('Key')

                    name = file_name
                    if name.startswith(prefix):
                        name = name[len(prefix):]
                        name = utils.append_start_path_sep(name)

                    name_hash = _get_name_hash(name)

                    file_size = utils.convert_value_to_type(remote_object.get('Size', None), to_type=int)
                    file_mtime = utils.convert_value_to_type(remote_object.get('LastModified', None),
                                                             to_type=datetime)
                    file_mtime = file_mtime.replace(tzinfo=pytz.UTC)
                    file_mtime = datetime.fromtimestamp(time.mktime(file_mtime.timetuple()))
                    file_hash = utils.convert_value_to_type(remote_object.get('ETag', None), to_type=str)

                    if file_hash is not None:
                        if file_hash.startswith('"'):
                            file_hash = file_hash[1:]
                        if file_hash.endswith('"'):
                            file_hash = file_hash[:-1]

                    file_info = {
                        INFO_FIELD_NAME: file_name,
                        INFO_FIELD_SIZE: file_size,
                        INFO_FIELD_MTIME: file_mtime,
                        INFO_FIELD_HASH: file_hash
                    }

                    operation_info = operations.setdefault(name_hash, {})

                    operation_info[INFO_OLD] = file_info
                    operation_info[INFO_OP] = OP_DELETE

            finally:
                if progress is not None:
                    del progress

        return operations

    def _load_vm_info(self, vm_uuid:VM_UUID ) -> Optional[Dict[str, Any]]:
        vm_uuid_s = convert_uuid_to_string(vm_uuid, True)

        builder = SQLBuilder()

        builder.set_statement_from(
            SQLBuilder.table(table_name=S3ParallelsOperation.TABLE, use_alias=True)
        )

        builder.set_statement_select(
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_CONFIG, table_name=S3ParallelsOperation.TABLE)
        )

        builder.set_statement_where(
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_UUID, table_name=S3ParallelsOperation.TABLE)
            )
        )

        record = self._database.execute_once(builder.make_select_statement(), vm_uuid_s)

        if record is not None:
            vm_info_packed: str = record.get_value(self.FIELD_CONFIG)
            vm_info_json: str = utils.decode_string(vm_info_packed)
            vm_info = json.loads(vm_info_json, cls=UUIDDecoder)

            return vm_info

        return None

    def _store_vm_info(self, vm_info: Dict[str, Any]) -> None:

        vm_uuid = vm_info.get('id')
        vm_uuid_s = convert_uuid_to_string(vm_uuid, True)

        builder = SQLBuilder()

        builder.set_statement_from(
            SQLBuilder.table(table_name=S3ParallelsOperation.TABLE, use_alias=True)
        )

        builder.set_statement_select(
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_ID, table_name=S3ParallelsOperation.TABLE)
        )

        builder.set_statement_where(
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_UUID, table_name=S3ParallelsOperation.TABLE)
            )
        )

        record = self._database.execute_once(builder.make_select_statement(), vm_uuid_s)

        vm_name = None
        vm_id = 0

        vm_info_json = json.dumps(vm_info, cls=UUIDEncoder, indent=4)
        vm_info_packed = utils.encode_string(vm_info_json)

        if record is not None:
            vm_id = record.field(S3ParallelsOperation.FIELD_ID).value

        if vm_id != 0:
            builder.reset()
            builder.set_statement_update(
                SQLBuilder.table(table_name=S3ParallelsOperation.TABLE)
            )
            builder.set_statement_settings(
                SQLBuilder.operator_equal_with_parameter(
                    SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_CONFIG)
                )
            )

            builder.set_statement_where(
                SQLBuilder.operator_equal_with_parameter(
                    SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_ID)
                )
            )

            self._database.execute_update(builder.make_update_statement(), [vm_info_packed, vm_id])
        elif vm_id == 0:
            fields = [S3ParallelsOperation.FIELD_UUID, S3ParallelsOperation.FIELD_CONFIG]
            values = ['?', '?']

            builder.reset()
            builder.set_statement_insert(table_name=S3ParallelsOperation.TABLE, statement=', '.join(fields))
            builder.set_statement_values(statement=', '.join(values))

            self._database.execute_update(builder.make_insert_statement(), [vm_uuid_s, vm_info_packed])

        return vm_name

    def _is_suspended(self, vm_id: VM_UUID) -> bool:
        if vm_id in self._suspended:
            return self._suspended[vm_id]
        return False

    def _resume(self, vm_id: VM_UUID):
        is_suspended = self._suspended.get(vm_id, False)
        if is_suspended:
            self.parallels.resume(vm_id=vm_id)
            del self._suspended[vm_id]

    def _suspend(self, vm_id: VM_UUID):
        status = self.parallels.get_status(vm_id=vm_id)
        if status in [VM_STATUS_RUNNING, VM_STATUS_PAUSED]:
            self.parallels.suspend(vm_id=vm_id)
            self._suspended[vm_id] = True

    def _update_snapshots(self, vm_id: VM_UUID):
        snapshot_list = self.parallels.get_snapshot_list(vm_id=vm_id)

        old_spanshot_list = [snapshot_id for snapshot_id, snapshot in snapshot_list.items() \
                             if snapshot.get('days', 0) >= VM_SNAPSHOT_DAYS_COUNT]

        if len(old_spanshot_list) > 0:
            for old_snapshot_id in old_spanshot_list:
                self.parallels.delete_snapshot(vm_id=vm_id, snap_id=old_snapshot_id)
            snapshot_list = self.parallels.get_snapshot_list(vm_id=vm_id)

        while len(snapshot_list) >= VM_SNAPSHOT_COUNT:
            older_snap = None

            for _, snapshot in snapshot_list.items():
                if not snapshot.get('current', False) and snapshot.get('state', '') != VM_SNAPSHOT_POWER_ON:

                    if older_snap is not None:
                        if snapshot.get('date', datetime.max) < older_snap.get('date', datetime.min):
                            older_snap = snapshot
                    else:
                        older_snap = snapshot

            if older_snap is not None:
                self.parallels.delete_snapshot(vm_id=vm_id, snap_id=older_snap.get('id'),
                                               delete_child=(older_snap.get('parent', '') == ''))
                # snapshot_list = self.parallels.get_snapshot_list(vm_id=vm_id)

        new_snapid = self.parallels.create_snapshot(vm_id=vm_id)

        if new_snapid is not None:
            # snapshot-switch <ID | NAME> -i,--id <snap_id> [--skip-resume]
            self.parallels.switch_snapshot(vm_id=vm_id, snapshot_id=new_snapid)

    def _run_process(self, vm_info: Dict[str, Any]):
        name = vm_info.get('name', None)
        if name is None:
            name = convert_uuid_to_string(vm_info.get('id'), use_curly_brackets=False)

        home = vm_info.get('home')

        if home is not None:
            locale_path = home

            home = utils.remove_end_path_sep(home)
            _, vm_type = os.path.splitext(home)

            if len(vm_type) > 0:
                vm_type = vm_type[1:].upper()

            if vm_type == VM_TYPE_PACKED:
                name = f'{name} (Packed)'
            if vm_type == VM_TYPE_ARCHIVED:
                name = f'{name} (Archived)'

            remote_path = f'Parallels/{name}'

            files = self._fetch_local_files_list(local_path=locale_path)
            files = self._fetch_remote_files_list(remote_path=remote_path, operations=files)

            if len(files) > 0:
                files = self._check_files(files)

                if len(files) > 0:
                    dt = datetime.now()

                    archive_name = name
                    archive_name = f'{dt.strftime("%H%M%S")} {archive_name}'
                    archive_path = utils.append_end_path_sep(os.path.join('Archives', dt.strftime('%Y/%j')))
                    archive_path = utils.remove_end_path_sep(archive_path)
                    archive_path = os.path.join(archive_path, archive_name)

                    self._copy_object(src=remote_path, dst=archive_path)
                    self._do_operation(files)

    def _check_exists_vm(self, vm_id: VM_UUID) -> bool:
        try:
            if self._load_vm_info(vm_uuid=vm_id) is not None:
                return True

            if self.parallels.check(vm_id=vm_id):
                return True
        except:
            pass
        return False

    def _do_run(self, vm_id: VM_UUID):
        if self._check_exists_vm(vm_id=vm_id):

            self._suspend(vm_id)
            try:

                self._pack_vm(vm_id)
                try:
                    vm_info = self._get_vm_info(vm_id=vm_id)

                    if vm_info is not None:
                        logger.info(
                            f'Uptime Virtual Machine {vm_info.get("name", "Unknown")} '
                            f'is {utils.time_to_string(vm_info.get("uptime", 0))}.'
                        )

                        vm_uuid = vm_info.get('id')
                        vm_home = vm_info.get('home')
                        vm_type = vm_info.get('type')
                        vm_name = vm_info.get('name')

                        logger.info(f"Found VM {vm_name} ({vm_type}) (vm_id={vm_uuid}).")
                        logger.info(f"Home path: {vm_home}")

                        self._run_process(vm_info=vm_info)

                        vm_info = self.parallels.get_vm_info(vm_id=vm_info.get('id'))
                        self._store_vm_info(vm_info=vm_info)
                finally:
                    self._unpack_vm(vm_id)

            finally:
                self._resume(vm_id=vm_id)
        else:
            raise VMUnknownUUIDError(vm_id=vm_id)

    def _pack_vm(self, vm_id: VM_UUID):
        if self.pack:
            self.parallels.pack_vm(vm_id)
            vm_id = convert_uuid_to_string(vm_id)
            self._packed[vm_id] = True

    def _is_packed(self, vm_id: VM_UUID):
        is_packed = self._packed.get(convert_uuid_to_string(vm_id))
        return is_packed

    def _unpack_vm(self, vm_id: VM_UUID):
        vm_id_s = convert_uuid_to_string(vm_id)
        is_packed = self._packed.get(vm_id_s, False)
        if is_packed:
            self.parallels.unpack_vm(vm_id)
            del self._packed[vm_id_s]

    def process(self, *args, **kwargs) -> None:
        if hasattr(self, '_storage'):
            del self._storage

        if self.vm_uuid is None:
            vm_id_list = self.parallels.get_vm_list()

            for vm_id in vm_id_list:
                self._do_run(vm_id=vm_id)
        else:
            self._do_run(vm_id=self.vm_uuid)

    @classmethod
    def execute(cls, *args, **kwargs):
        bucket_name = kwargs.pop('bucket', False)

        o = cls(bucket=bucket_name)

        try:
            o.force = kwargs.pop('force', False)
            o.pack = kwargs.pop('pack', False)
            o.show_progress = kwargs.pop('show_progress', True)
            o.vm_uuid = kwargs.pop('vm_id', True)

            o.process(*args, **kwargs)
        finally:
            del o


class S3ParallelsBackup(S3ParallelsOperation):

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_BACKUP
        return super(S3ParallelsBackup, self)._compare_files(file_info_old, file_info_new)

    def _do_file_operation(self, local_file_info: Dict[str, Any], remote_file_info: Dict[str, Any]):
        local_file_name = local_file_info.get('name')
        remote_file_name = remote_file_info.get('name')
        logger.info(f'Upload file {remote_file_name}...')
        self.storage.upload_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def _do_operation(self, files: Dict[str, Any]):
        remote_files_list = self._get_remote_files_list(files)
        if len(remote_files_list) > 0:
            objects_for_delete = [{'Key': info.get(INFO_FIELD_NAME)} for info in remote_files_list]

            if len(objects_for_delete) > 0:
                self.storage.delete_objects(objects=objects_for_delete)

        for _, operation_info in files.items():
            local_file = operation_info.get(INFO_NEW)
            remote_file = operation_info.get(INFO_OLD)

            self._do_file_operation(local_file_info=local_file, remote_file_info=remote_file)

    def _run_process(self, vm_info: Dict[str, Any]):
        super(S3ParallelsBackup, self)._run_process(vm_info=vm_info)
        vm_id = vm_info.get('id', uuid.uuid4())
        if not self._is_packed(vm_id=vm_id):
            self._do_update_snapshots(vm_id=vm_id)

    def _do_update_snapshots(self, vm_id: VM_UUID):
        snapshot_list = self.parallels.get_snapshot_list(vm_id=vm_id)

        need_create_new_shapshot = True

        if len(snapshot_list) > 0:
            need_create_new_shapshot = False

            snapshot_parent = None
            snapshot_current = None
            snapshot_first = None
            snapshot_last = None

            for _, snapshot in snapshot_list.items():
                if snapshot.get('parent', '') == '':
                    snapshot_parent = snapshot

                if snapshot.get('current', False):
                    snapshot_current = snapshot

                if snapshot_first is None:
                    snapshot_first = snapshot
                elif snapshot_first.get('date', datetime.min) > snapshot.get('date', datetime.max):
                    snapshot_first = snapshot

                if snapshot_last is None:
                    snapshot_last = snapshot
                elif snapshot_last.get('date', datetime.max) < snapshot.get('date', datetime.min):
                    snapshot_last = snapshot

            if snapshot_first is not None:
                if snapshot_first.get('days', 0) > VM_SNAPSHOT_DAYS_COUNT:
                    self.parallels.delete_snapshot(vm_id=vm_id,
                                                   snap_id=snapshot_first.get('id', ''),
                                                   delete_child=snapshot_first.get('parent', '') == '')

                    if snapshot_first.get('parent') == '':
                        snapshot_parent = None
                        snapshot_current = None
                        snapshot_first = None
                        snapshot_last = None

                        need_create_new_shapshot = True
                    else:
                        if snapshot_current.get('id') == snapshot_first.get('id'):
                            snapshot_current = None

                        if snapshot_parent.get('id') == snapshot_first.get('id'):
                            snapshot_parent = None

            if snapshot_current is None and snapshot_last is not None:
                self.parallels.switch_snapshot(vm_id=vm_id, snapshot_id=snapshot_last.get('id'))
                snapshot_current = snapshot_last

        if need_create_new_shapshot:
            self.parallels.create_snapshot(vm_id=vm_id)


class S3ParallelsRestore(S3ParallelsOperation):

    def _copy_object(self, src: str, dst: str) -> None:
        return

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_RESTORE

        return super(S3ParallelsRestore, self)._compare_files(file_info_old, file_info_new)

    def _do_file_operation(self, local_file_info: Dict[str, Any], remote_file_info: Dict[str, Any]):
        local_file_name = local_file_info.get('name')
        remote_file_name = remote_file_info.get('name')
        logger.info(f'Download file {remote_file_name}...')
        self.storage.download_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def _do_operation(self, files: Dict[str, Any]):
        local_files_list = self._get_local_files_list(files)
        local_files_list = [info.get(INFO_FIELD_NAME) for info in local_files_list]

        for local_file in local_files_list:
            if os.path.exists(local_file):
                os.remove(local_file)

        for _, operation_info in files:
            local_file = operation_info.get(INFO_NEW)
            remote_file = operation_info.get(INFO_OLD)

            self._do_file_operation(local_file_info=local_file, remote_file_info=remote_file)

    def _pack_vm(self, vm_id: VM_UUID):
        return

    def _unpack_vm(self, vm_id: VM_UUID):
        return