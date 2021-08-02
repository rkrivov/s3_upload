#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

import pytz

from common import dbase
from common.app_logger import get_logger
from common.consts import WORK_FOLDER
from common.convertors import append_start_path_sep, append_end_path_sep, \
    remove_start_path_sep, convert_value_to_type, \
    decode_string, encode_string, remove_end_path_sep, time_to_string
from common.dbase import SQLBuilder
from common.notify import notify
from common.utils import print_progress_bar, get_terminal_width
from s3._base._base import S3Base, _get_name_hash, _get_file_info, INFO_NEW, INFO_OP, OP_INSERT, INFO_FIELD_NAME, \
    INFO_FIELD_SIZE, INFO_FIELD_MTIME, INFO_FIELD_HASH, OP_DELETE, INFO_OLD
from s3._base._consts import VM_STATUS_RUNNING, VM_STATUS_PAUSED, VM_SNAPSHOT_DAYS_COUNT, VM_SNAPSHOT_COUNT, \
    VM_SNAPSHOT_POWER_ON, VM_TYPE_PACKED, VM_TYPE_ARCHIVED
from s3._base._typing import VM_UUID
from s3.parallels.errors import VMUnknownUUIDError
from s3.parallels.parallels import Parallels
from s3.parallels.uuid_decoder import UUIDDecoder
from s3.parallels.uuid_encoder import UUIDEncoder
from s3.utils import convert_uuid_to_string

logger = get_logger(__name__)


class S3ParallelsOperation(S3Base):
    DATABASE = os.path.join(WORK_FOLDER, 'cache/parallels.vm.cache')
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
                name = append_start_path_sep(name)

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
                            append_end_path_sep(root),
                            remove_start_path_sep(file)
                        )

                        name = local_file_path
                        if name.startswith(local_path):
                            name = name[len(local_path):]
                            name = append_start_path_sep(name)

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

        prefix = append_end_path_sep(remote_path)
        objects_count = self.storage.get_objects_count(prefix=prefix)

        if objects_count > 0:
            loaded_objects_count = 0

            for remote_object in self.storage.fetch_bucket_objects(prefix=prefix):
                if self.show_progress:
                    loaded_objects_count += 1
                    print_progress_bar(iteration=loaded_objects_count,
                                       total=objects_count,
                                       prefix=f'Feching remote objects froom {remote_path}')

                file_name = remote_object.get('Key')

                name = file_name
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    name = append_start_path_sep(name)

                name_hash = _get_name_hash(name)

                file_size = convert_value_to_type(remote_object.get('Size', None), to_type=int)
                file_mtime = convert_value_to_type(remote_object.get('LastModified', None),
                                                   to_type=datetime)
                file_mtime = file_mtime.replace(tzinfo=pytz.UTC)
                file_mtime = datetime.fromtimestamp(time.mktime(file_mtime.timetuple()))
                file_hash = convert_value_to_type(remote_object.get('ETag', None), to_type=str)

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

        return operations

    def _load_vm_info(self, vm_uuid: VM_UUID) -> Optional[Dict[str, Any]]:
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
            vm_info_json: str = decode_string(vm_info_packed)
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
        vm_info_packed = encode_string(vm_info_json)

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

    def _notify(self, message):
        notify(message=message, title='s3_upload', subtitle=type(self).__name__);

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

            home = remove_end_path_sep(home)
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
                    archive_path = append_end_path_sep(os.path.join('Archives', dt.strftime('%Y/%j')))
                    archive_path = remove_end_path_sep(archive_path)
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
                            f'is {time_to_string(vm_info.get("uptime", 0))}.'
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
            self._notify(f'We begin to pack VM {convert_uuid_to_string(vm_id)}...')
            self.parallels.pack_vm(vm_id)
            vm_id = convert_uuid_to_string(vm_id)
            self._packed[vm_id] = True
            self._notify(f'We finished packing VM {convert_uuid_to_string(vm_id)}...')

    def _is_packed(self, vm_id: VM_UUID):
        is_packed = self._packed.get(convert_uuid_to_string(vm_id))
        return is_packed

    def _unpack_vm(self, vm_id: VM_UUID):
        vm_id_s = convert_uuid_to_string(vm_id)
        is_packed = self._packed.get(vm_id_s, False)
        if is_packed:
            self._notify(f'We begin to unpack VM {convert_uuid_to_string(vm_id)}...')
            self.parallels.unpack_vm(vm_id)
            del self._packed[vm_id_s]
            self._notify(f'We finished unpacking VM {convert_uuid_to_string(vm_id)}...')

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
