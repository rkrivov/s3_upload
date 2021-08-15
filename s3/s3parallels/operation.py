#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import asyncio
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any, Text
from uuid import UUID

import pytz

from s3.s3base.s3baseobject import S3Base, get_name_hash, get_file_info, INFO_LOCAL, INFO_OP, OP_INSERT, \
    INFO_FIELD_NAME, \
    INFO_FIELD_SIZE, INFO_FIELD_MTIME, INFO_FIELD_HASH, OP_DELETE, INFO_REMOTE, OP_UPDATE
from s3.s3base.s3consts import VM_STATUS_RUNNING, VM_STATUS_PAUSED, VM_SNAPSHOT_DAYS_COUNT, VM_SNAPSHOT_COUNT, \
    VM_SNAPSHOT_POWER_ON, VM_TYPE_PACKED, VM_TYPE_ARCHIVED
from s3.s3base.s3typing import VirtualMachineID
from s3.s3functions import convert_uuid_to_string, remote_brackets
from s3.s3parallels.errors import VMUnknownUUIDError
from s3.s3parallels.objects.virtualmachine import ParallelsVirtualMachine
from s3.s3parallels.parallels import Parallels
from s3.s3parallels.uuid_decoder import UUIDDecoder
from s3.s3parallels.uuid_encoder import UUIDEncoder
from utils import consts
from utils import dbase
from utils.app_logger import get_logger
from utils.asyncobject import AsyncObjectHandler
from utils.convertors import append_end_path_sep, \
    remove_start_path_sep, convert_value_to_type, \
    decode_string, encode_string, remove_end_path_sep, time_to_short_string, make_template_from_string, \
    make_string_from_template, time_to_string
from utils.dbase import SQLBuilder
from utils.files import calc_file_hash
from utils.files_multi_threads import ScanFolder
from utils.functions import print_progress_bar, get_parameter, is_equal

logger = get_logger(__name__)


def get_virtual_machine_type(virtual_machine: ParallelsVirtualMachine) -> Optional[str]:
    virtual_machine_home = virtual_machine.get('home')

    virtual_machine_home = remove_end_path_sep(virtual_machine_home)
    _, virtual_machine_type = os.path.splitext(virtual_machine_home)

    if len(virtual_machine_type) > 0:
        virtual_machine_type = virtual_machine_type[1:].upper()

        return virtual_machine_type

    return None


class S3ParallelsOperation(S3Base, AsyncObjectHandler):
    DATABASE = os.path.join(consts.CACHES_FOLDER, 'parallels.cache')
    TABLE = "virtual_machines"

    FIELD_ID = 'id'
    FIELD_UUID = 'uuid'
    FIELD_CONFIG = 'config'


    FILES_CACHE_TABLE = 'files_cache'
    FIELD_FILE_NAME = 'file_name'
    FIELD_FILE_SIZE = 'file_size'
    FIELD_FILE_MTIME = 'file_mtime'
    FIELD_FILE_HASH = 'file_hash'
    FIELD_FILE_CONTROL_HASH = 'file_control_hash'

    SCRIPT = f"""
    --- The script was written by Roman N. Krivov a.k.a. Eochaid Bres Drow
    --- on {datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S %Z')} 

    --- Create table {TABLE} if it doesn't exists
    CREATE TABLE IF NOT EXISTS {TABLE} (
        {FIELD_ID} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        {FIELD_UUID} TEXT NOT NULL,
        {FIELD_CONFIG} TEXT);

    --- Create table {FILES_CACHE_TABLE} if it doesn't exists
    CREATE TABLE IF NOT EXISTS {FILES_CACHE_TABLE} (
        {FIELD_FILE_NAME} TEXT PRIMARY KEY NOT NULL,
        {FIELD_FILE_SIZE} INTEGER NOT NULL,
        {FIELD_FILE_MTIME} TIMESPAMP,
        {FIELD_FILE_HASH} TEXT,
        {FIELD_FILE_CONTROL_HASH} TEXT);

    --- Create indexes for table {TABLE}
    CREATE UNIQUE INDEX IF NOT EXISTS {TABLE}_{FIELD_UUID}_idx ON {TABLE} ({FIELD_UUID});

    --- Create indexes for table {FILES_CACHE_TABLE}
    CREATE UNIQUE INDEX IF NOT EXISTS {FILES_CACHE_TABLE}_{FIELD_FILE_NAME}_idx ON {FILES_CACHE_TABLE} ({FIELD_FILE_NAME});
    CREATE INDEX IF NOT EXISTS {FILES_CACHE_TABLE}_{FIELD_FILE_SIZE}_idx ON {FILES_CACHE_TABLE} ({FIELD_FILE_SIZE});
    CREATE INDEX IF NOT EXISTS {FILES_CACHE_TABLE}_{FIELD_FILE_MTIME}_idx ON {FILES_CACHE_TABLE} ({FIELD_FILE_MTIME});
    CREATE INDEX IF NOT EXISTS {FILES_CACHE_TABLE}_{FIELD_FILE_HASH}_idx ON {FILES_CACHE_TABLE} ({FIELD_FILE_HASH});
    CREATE INDEX IF NOT EXISTS {FILES_CACHE_TABLE}_{FIELD_FILE_CONTROL_HASH}_idx ON {FILES_CACHE_TABLE} ({FIELD_FILE_CONTROL_HASH});
    """

    def __init__(self, bucket: str, **kwargs):
        self._database = dbase.create_database(name=S3ParallelsOperation.DATABASE)
        self._database.execute_script(S3ParallelsOperation.SCRIPT)
        self._scan_folder = ScanFolder()
        self._scan_folder.daemon = True

        self._force = kwargs.pop('force', False)
        self._need_archive = kwargs.pop('archive', False)
        self._need_pack = kwargs.pop('pack', False)
        self._show_progress = kwargs.pop('show_progress', True)
        self._vm_uuid = kwargs.pop('virtual_machine_id', True)

        self._parallels = None

        self.__current_date_time = datetime.now()

        self._archived = {}
        self._suspended = {}
        self._packed = {}
        # self._main_event_loop = asyncio.get_event_loop()

        if self._need_archive and self._need_pack:
            raise Exception(f"{type(self).__name__} initialize failure: you need to use either packing or archiving")

        super(S3ParallelsOperation, self).__init__(bucket=bucket)

    def __del__(self):
        # if self._main_event_loop.is_running():
        #     self._main_event_loop.run_forever()
        # self._main_event_loop.close()

        if hasattr(self, '_database'):
            if self._database is not None:
                del self._database

        super(S3ParallelsOperation, self).__del__()

    @property
    def parallels(self) -> Parallels:
        if not hasattr(self, '_parallels'):
            self._parallels = None

        if self._parallels is None:
            self._init_parallels_object()

        return self._parallels

    @property
    def vm_uuid(self) -> UUID:
        ret = None

        if hasattr(self, '_vm_uuid'):
            ret = self._vm_uuid

        return ret

    @vm_uuid.setter
    def vm_uuid(self, value: UUID):
        self._vm_uuid = value

    def _is_exists(self, file_info: Dict[str, Any]) -> bool:
        builder = SQLBuilder()

        builder.set_statement_select(
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
        )

        builder.set_statement_from(
            SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE)
        )

        builder.set_statement_where(
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(S3ParallelsOperation.FIELD_FILE_NAME)
            )
        )

        record = self._database.execute_once(builder.make_select_statement(), file_info.get(INFO_FIELD_NAME))

        if record is not None:
            if record.first.value == file_info.get(INFO_FIELD_NAME):
                return True

        return False

    async def _insert_file_to_dbase_async(self, file_info: Dict[str, Any]):
        self._insert_file_to_dbase(file_info=file_info)

    async def _update_file_in_dbase_async(self, file_info: Dict[str, Any]):
        self._update_file_in_dbase(file_info=file_info)

    async def _delete_file_from_dbase_async(self, file_info: Dict[str, Any]):
        self._insert_file_to_dbase(file_info=file_info)

    def _insert_file_to_dbase(self, file_info: Dict[str, Any]):
        if self._is_exists(file_info=file_info):
            raise AttributeError(f'File "{file_info.get(INFO_FIELD_NAME)}" already exists in dbase.')

        builder = SQLBuilder()

        fields_list = [
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME),
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_SIZE),
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_MTIME),
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_HASH)
        ]

        values_list = ['?'] * len(fields_list)

        builder.set_statement_insert(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE), ', '.join(fields_list))
        builder.set_statement_values(', '.join(values_list))

        values = [
            file_info.get(INFO_FIELD_NAME),
            file_info.get(INFO_FIELD_SIZE),
            file_info.get(INFO_FIELD_MTIME),
            file_info.get(INFO_FIELD_HASH),
        ]

        inserted_files = self._database.execute_update(builder.make_insert_statement(), tuple(values))
        self._inserted_new_files += inserted_files

    def _update_file_in_dbase(self, file_info: Dict[str, Any]):
        if not self._is_exists(file_info=file_info):
            raise AttributeError(f'File "{file_info.get(INFO_FIELD_NAME)}" is not exists in dbase.')

        builder = SQLBuilder()

        builder.set_statement_update(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE))
        builder.set_statement_settings([
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_SIZE)
            ),
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_MTIME)
            ),
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_HASH)
            ),
        ])
        builder.set_statement_where(
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
            )
        )

        values = [
            file_info.get(INFO_FIELD_SIZE),
            file_info.get(INFO_FIELD_MTIME),
            file_info.get(INFO_FIELD_HASH),
            file_info.get(INFO_FIELD_NAME)
        ]

        updated_files = self._database.execute_update(builder.make_update_statement(), tuple(values))
        self._updated_files += updated_files

    def _delete_file_from_dbase(self, file_info: Dict[str, Any]):
        if not self._is_exists(file_info=file_info):
            raise AttributeError(f'File "{file_info.get(INFO_FIELD_NAME)}" is not exists in dbase.')

        builder = SQLBuilder()

        builder.set_statement_delete(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE))
        builder.set_statement_where(
            SQLBuilder.operator_equal_with_parameter(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
            )
        )

        deleted_files = self._database.execute_update(builder.make_delete_statement(), [file_info.get(INFO_FIELD_NAME)])
        self._deleted_files += deleted_files

    def _fetch_local_files_list(self, local_path: str = None,
                                operations: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._init_cache()

        if local_path is None:
            local_path = self._local_path

        if operations is None:
            operations = {}

        if os.path.isfile(local_path):
            local_file_name = local_path
            local_path = os.path.dirname(local_file_name)

            name = local_file_name

            name = make_template_from_string(name, path=remove_end_path_sep(self._local_path))

            name_hash = get_name_hash(name)

            file_info = get_file_info(file=local_file_name)

            operation_info = operations.setdefault(name_hash, {})
            operation_info[INFO_LOCAL] = file_info
            operation_info[INFO_OP] = OP_INSERT
        else:
            for root, dirs, files in os.walk(self._local_path):
                for file in files:
                    if not file.startswith('.') and not file.startswith('~'):
                        local_file_path = os.path.join(
                            append_end_path_sep(root),
                            remove_start_path_sep(file)
                        )

                        file_info = get_file_info(file=local_file_path)

                        name = local_file_path
                        name = make_template_from_string(name, path=remove_end_path_sep(self._local_path))

                        file_info[INFO_FIELD_NAME] = name

                        name_hash = get_name_hash(name)

                        operation_info = operations.setdefault(name_hash, {})

                        need_calc_hash = False
                        operation_name = ''

                        file_info_remote = operation_info.get(INFO_REMOTE, None)
                        if file_info_remote is None:
                            need_calc_hash = True
                            operation_name = OP_INSERT
                        else:
                            file_size_remote = file_info_remote.get(INFO_FIELD_SIZE, 0)
                            file_size_local = file_info.get(INFO_FIELD_SIZE, 0)

                            file_mtime_remote = file_info_remote.get(INFO_FIELD_MTIME, datetime.min)
                            file_mtime_local = file_info.get(INFO_FIELD_MTIME,datetime.min)

                            if not is_equal(file_size_remote, file_size_local) and \
                                    not is_equal(file_mtime_remote, file_mtime_local):
                                need_calc_hash = True
                                operation_name = OP_UPDATE

                        if need_calc_hash:
                            file_info[INFO_FIELD_HASH] = self._calc_hash(local_file_path)

                            operation_info[INFO_LOCAL] = file_info
                            operation_info[INFO_OP] = operation_name
                        else:
                            del operations[name_hash]

        return operations

    def _get_cached_files_count(self):
        builder = SQLBuilder()

        builder.set_statement_from(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE, use_alias=True))
        builder.set_statement_select(SQLBuilder.field(field_name=SQLBuilder.sql_function_count('*'), use_alias=True))

        record = self._database.execute_once(builder.make_select_statement())

        num_of_records = 0
        if record is not None:
            num_of_records = record.first.value

        return num_of_records

    def _store_files_to_cache_from_remote(self, remote_path: str):
        builder = SQLBuilder()

        fields_list = [
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME),
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_SIZE),
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_MTIME),
            SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_HASH)
        ]

        values_list = ['?'] * len(fields_list)

        builder.set_statement_insert(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE), ', '.join(fields_list))
        builder.set_statement_values(', '.join(values_list))

        prefix = append_end_path_sep(remote_path)
        objects_count = self.storage.get_objects_count(prefix=prefix)

        if objects_count > 0:
            loaded_objects_count = 0
            files_to_insert = []
            for remote_object in self.storage.fetch_bucket_objects(prefix=prefix):
                if self.show_progress:
                    loaded_objects_count += 1
                    print_progress_bar(iteration=loaded_objects_count,
                                       total=objects_count,
                                       prefix='Fetching remote objects.py')

                file_name = remote_object.get('Key')
                file_name = make_template_from_string(file_name, path=remove_end_path_sep(remote_path))

                file_size = convert_value_to_type(remote_object.get('Size', None), to_type=int)
                file_mtime = convert_value_to_type(remote_object.get('LastModified', None),
                                                   to_type=datetime)
                file_mtime = file_mtime.replace(tzinfo=pytz.UTC)
                file_mtime = datetime.fromtimestamp(time.mktime(file_mtime.timetuple()))
                file_hash = convert_value_to_type(remote_object.get('ETag', None), to_type=str)

                if file_hash is not None:
                    file_hash = remote_brackets(file_hash, '"')
                files_to_insert.append((file_name, file_size, file_mtime, file_hash, ))

            if len(files_to_insert):
                self._database.execute_update(builder.make_insert_statement(), files_to_insert)

    def _fetch_remote_files_list(self, remote_path: str = None,
                                 operations: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        if remote_path is None:
            remote_path = self._archive_path

        if operations is None:
            operations = {}

        num_of_records = self._get_cached_files_count()
        if num_of_records == 0:
            self._store_files_to_cache_from_remote(remote_path=remote_path)
            num_of_records = self._get_cached_files_count()

        if num_of_records > 0:
            builder = SQLBuilder()

            builder.set_statement_select([
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME),
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_SIZE),
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_MTIME),
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_HASH)
            ])

            builder.set_statement_from(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE))

            for record in self._database.execute(builder.make_select_statement()):
                name_hash = get_name_hash(record.field(S3ParallelsOperation.FIELD_FILE_NAME).value)

                file_info = {
                    INFO_FIELD_NAME: convert_value_to_type(record.field(S3ParallelsOperation.FIELD_FILE_NAME).value,
                                                           to_type=str),
                    INFO_FIELD_SIZE: convert_value_to_type(record.field(S3ParallelsOperation.FIELD_FILE_SIZE).value,
                                                           to_type=int),
                    INFO_FIELD_MTIME: convert_value_to_type(record.field(S3ParallelsOperation.FIELD_FILE_MTIME).value,
                                                            to_type=datetime),
                    INFO_FIELD_HASH: convert_value_to_type(record.field(S3ParallelsOperation.FIELD_FILE_HASH).value,
                                                           to_type=Text)
                }

                operation_info = operations.setdefault(name_hash, {})

                operation_info[INFO_REMOTE] = file_info
                operation_info[INFO_OP] = OP_DELETE

        return operations

    def _init_parallels_object(self) -> Parallels:
        self._parallels = Parallels()
        return self._parallels

    def _copy_to_archive(self):
        for fetched_object in self.storage.fetch_bucket_objects(prefix=self._archive_path):
            src_file_name = fetched_object.get('Key')
            tmp_file_name = make_template_from_string(src_file_name, path=remove_end_path_sep(self._archive_path))
            dst_file_name = make_string_from_template(tmp_file_name, path=remove_end_path_sep(self._bak_archive_path))
            self.append_task_to_list(self.storage.copy_object_async(src_file_name, dst_file_name))
        self.run_task_list()

    def get_virtual_machine_info(self, virtual_machine_id: VirtualMachineID) -> Optional[Dict[str, Any]]:
        vm_info = self.load_virtual_machine_info(virtual_machine_id=virtual_machine_id)

        if vm_info is None:
            try:
                vm_info = self.parallels.get_virtual_machine(virtual_machine_id=virtual_machine_id)
            except:
                vm_info = None

        return vm_info

    def load_virtual_machine_info(self, virtual_machine_id: VirtualMachineID) -> Optional[Dict[str, Any]]:
        self._database.callback_tracebacks_enabled = False
        try:
            vm_uuid_s = convert_uuid_to_string(virtual_machine_id, True)

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
        finally:
            self._database.callback_tracebacks_enabled = True

    def store_virtual_machine_info(self, virtual_machine: ParallelsVirtualMachine) -> None:
        self._database.callback_tracebacks_enabled = False
        try:
            vm_uuid = virtual_machine.get('id')
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

            vm_info_json = json.dumps(virtual_machine.dict(), cls=UUIDEncoder, indent=4)
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
        finally:
            self._database.callback_tracebacks_enabled = True

    def _is_suspended(self, virtual_machine_id: VirtualMachineID) -> bool:
        if virtual_machine_id in self._suspended:
            return self._suspended[virtual_machine_id]
        return False

    def resume_virtual_machine(self, virtual_machine_id: VirtualMachineID):
        is_suspended = self._suspended.get(virtual_machine_id, False)
        if is_suspended:
            self.parallels.resume_virtual_machine(virtual_machine_id=virtual_machine_id)
            del self._suspended[virtual_machine_id]

    def suspend_virtual_machine(self, viirtual_machine_id: VirtualMachineID):
        status = self.parallels.get_virtual_machine_status(virtual_machine_id=viirtual_machine_id)
        if status in [VM_STATUS_RUNNING, VM_STATUS_PAUSED]:
            self.parallels.suspend_virtual_machine(virtual_machine_id=viirtual_machine_id)
            self._suspended[viirtual_machine_id] = True

    def update_snapshots(self, virtual_machine_id: VirtualMachineID):
        snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)

        old_spanshot_list = [snapshot_id for snapshot_id, snapshot in snapshot_list if snapshot.days >= VM_SNAPSHOT_DAYS_COUNT]

        if len(old_spanshot_list) > 0:
            for old_snapshot_id in old_spanshot_list:
                self.parallels.delete_snapshot(virtual_machine_id=virtual_machine_id, snapshot_id=old_snapshot_id)
            snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)

        while len(snapshot_list) >= VM_SNAPSHOT_COUNT:
            older_snap = None

            for snapshot in snapshot_list:
                if not snapshot.get('current', False) and snapshot.get('state', '') != VM_SNAPSHOT_POWER_ON:

                    if older_snap is not None:
                        if snapshot.get('date', datetime.max) < older_snap.get('date', datetime.min):
                            older_snap = snapshot
                    else:
                        older_snap = snapshot

            if older_snap is not None:
                self.parallels.delete_snapshot(virtual_machine_id=virtual_machine_id, snapshot_id=older_snap.get('id'),
                                               delete_child=(older_snap.get('parent', '') == ''))
                snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)

        new_snapid = self.parallels.create_snapshot(virtual_machine_id=virtual_machine_id)

        if new_snapid is not None:
            # snapshot-switch <ID | NAME> -i,--id <snapshot_id> [--skip-resume_virtual_machine]
            self.parallels.switch_snapshot(virtual_machine_id=virtual_machine_id, snapshot_id=new_snapid)

    def _init_path(self, virtual_machine: ParallelsVirtualMachine) -> None:
        virtual_machine_id = virtual_machine.get('id')
        virtual_machine_home = virtual_machine.get('home')
        virtual_machine_type = get_virtual_machine_type(virtual_machine)
        virtual_machine_name = convert_uuid_to_string(vm_id=virtual_machine_id, use_curly_brackets=False)
        virtual_machine_remote_path = append_end_path_sep(virtual_machine_name)

        file_name = virtual_machine_home

        if file_name.startswith(self.parallels.parallels_home_path):
            file_name = file_name[len(self.parallels.parallels_home_path):]
            file_name = remove_start_path_sep(file_name)
        else:
            raise ValueError(f"Incorrect virtual machine home path ({virtual_machine_home}).")

        if virtual_machine_type is not None:
            if virtual_machine_type == VM_TYPE_PACKED:
                virtual_machine_remote_path = os.path.join('Packed/', virtual_machine_remote_path)
                virtual_machine_remote_path = append_end_path_sep(virtual_machine_remote_path)
            elif virtual_machine_type == VM_TYPE_ARCHIVED:
                virtual_machine_remote_path = os.path.join('Archived/', virtual_machine_remote_path)
                virtual_machine_remote_path = append_end_path_sep(virtual_machine_remote_path)
            else:
                virtual_machine_remote_path = os.path.join('Standard/', virtual_machine_remote_path)
                virtual_machine_remote_path = append_end_path_sep(virtual_machine_remote_path)
        else:
            virtual_machine_remote_path = os.path.join('Standard/', virtual_machine_remote_path)
            virtual_machine_remote_path = append_end_path_sep(virtual_machine_remote_path)

        remote_path = os.path.join(
            'Parallels/',
            remove_start_path_sep(virtual_machine_remote_path)
        )

        backup_remote_path = os.path.join(
            'Archives/',
            remove_start_path_sep(virtual_machine_remote_path)
        )

        remote_path = os.path.join(
            append_end_path_sep(remote_path),
            append_end_path_sep(file_name)
        )

        backup_remote_path = append_end_path_sep(backup_remote_path)

        backup_remote_path = os.path.join(
            backup_remote_path,
            datetime.now().strftime("%Y/%j/%H%M")
        )

        backup_remote_path = append_end_path_sep(backup_remote_path)

        backup_remote_path = os.path.join(
            append_end_path_sep(backup_remote_path),
            append_end_path_sep(file_name)
        )

        self.set_local_path(virtual_machine_home)
        self.set_archive_path(remote_path)
        self.set_backup_archive_path(backup_remote_path)

    def _init_cache(self):
        start_time = time.time()
        try:
            self._scan_folder(folder=self._local_path)
            self._scan_folder.wait()

            if not hasattr(self, '_hash_cache'):
                self._hash_cache = {}

            self._hash_cache.clear()

            for file_name, _ in self._scan_folder.files_list.items():
                self._hash_cache[file_name] = self._scan_folder.get_hash(file_name)

        finally:
            print(f"Elapsed is {time_to_string(time.time() - start_time, human=True)} ({len(self._hash_cache)} file(s))")

    def _process_with_virtual_machine(self, virtual_machine: ParallelsVirtualMachine) -> None:
        self._init_path(virtual_machine=virtual_machine)

        files = self.fetch_files()

        if len(files) > 0:
            self._copy_to_archive()
            self.operation(files, virtual_machine=virtual_machine)

    def _check_virtual_machine(self, virtual_machine_id: VirtualMachineID) -> bool:

        try:
            if self.load_virtual_machine_info(virtual_machine_id=virtual_machine_id) is not None:
                return True

            if self.parallels.check(virtual_machine_id=virtual_machine_id):
                return True

        except:
            pass

        return False

    def _process(self, virtual_machine: ParallelsVirtualMachine):

        if virtual_machine is None:
            raise AttributeError('Virtual machine information is missing.')

        virtual_machine_id = virtual_machine.id

        if virtual_machine_id is None:
            raise ValueError('Virtual machine identifier is missing.')

        if self._check_virtual_machine(virtual_machine_id=virtual_machine_id):

            self.suspend_virtual_machine(viirtual_machine_id=virtual_machine_id)
            try:
                virtual_machine_id = virtual_machine.id
                virtual_machine_type = virtual_machine.type
                virtual_machine_name = virtual_machine.name
                virtual_machine_uptime = virtual_machine.uptime

                logger.info(
                    f"Found {virtual_machine_type} {virtual_machine_name} with identifier {virtual_machine_id} "
                    f"and uptime is {time_to_short_string(virtual_machine_uptime)}."
                )

                self._process_with_virtual_machine(virtual_machine=virtual_machine)

                vm_info = self.parallels.get_virtual_machine(virtual_machine_id=virtual_machine_id)

                self.store_virtual_machine_info(virtual_machine=vm_info)
            finally:
                self.resume_virtual_machine(virtual_machine_id=virtual_machine_id)

        else:
            raise VMUnknownUUIDError(vm_id=virtual_machine_id)

    def _update_parameters(self, **kwargs):
        self.vm_uuid = kwargs.get('virtual_machine_id', None)

        super(S3ParallelsOperation, self)._update_parameters(**kwargs)

    def _upgrade_database(self):
        builder = SQLBuilder()

        builder.set_statement_select(SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME))
        builder.set_statement_from(SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE))
        builder.set_statement_where(
            SQLBuilder.operator_is(
                SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_CONTROL_HASH),
                SQLBuilder.value(None)
            )
        )

        files_for_update = []

        for record in self._database.execute(builder.make_select_statement()):
            file_name = record.field(S3ParallelsOperation.FIELD_FILE_NAME).value
            local_file_name = make_string_from_template(file_name, path=remove_end_path_sep(self._local_path))
            if os.path.exists(local_file_name):
                hash = calc_file_hash(file_object=local_file_name, hash_name='sha512', show_progress=self.show_progress)
                files_for_update.append(tuple([hash, file_name]))

        if len(files_for_update) > 0:
            builder.reset()
            builder.set_statement_update(
                SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE)
            )
            builder.set_statement_settings(
                SQLBuilder.operator_equal_with_parameter(
                    SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_CONTROL_HASH)
                )
            )
            builder.set_statement_where(
                SQLBuilder.operator_and(
                    SQLBuilder.operator_equal_with_parameter(
                        SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
                    ),
                    SQLBuilder.operator_is(
                        SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_CONTROL_HASH),
                        SQLBuilder.value(None)
                    )
                )
            )

            self._database.execute_update(builder.make_update_statement(), files_for_update)
        # builder = SQLBuilder()
        #
        # builder.set_statement_select(
        #     SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
        # )
        #
        # builder.set_statement_from(
        #     SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE)
        # )
        #
        # file_names = []
        #
        # for record in self._database.execute(builder.make_select_statement()):
        #     file_names.append(record.field(S3ParallelsOperation.FIELD_FILE_NAME).value)
        #
        # file_names_for_update = []
        #
        # for file_name in file_names:
        #     updated_file_name = re.sub(r"(%(\w+)%)", lambda m: "${" + m.group(2).upper() + "}", file_name, flags=re.IGNORECASE)
        #     if not is_equal(updated_file_name, file_name):
        #         file_names_for_update.append((updated_file_name, file_name, ))
        #
        # if len(file_names_for_update) > 0:
        #     builder.reset()
        #
        #     builder.set_statement_update(
        #         SQLBuilder.table(table_name=S3ParallelsOperation.FILES_CACHE_TABLE)
        #     )
        #     builder.set_statement_settings(
        #         SQLBuilder.operator_equal_with_parameter(
        #             SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
        #         )
        #     )
        #
        #     builder.set_statement_where(
        #         SQLBuilder.operator_equal_with_parameter(
        #             SQLBuilder.field(field_name=S3ParallelsOperation.FIELD_FILE_NAME)
        #         )
        #     )
        #
        #     self._database.execute_update(builder.make_update_statement(), file_names_for_update)

    def process(self):
        virtual_machine_id_list = []

        if self.vm_uuid is not None:
            virtual_machine_id_list.append(self.vm_uuid)

        for virtual_machine in self.parallels.get_virtual_machine_iter(virtual_machine_id_list):
            self._process(virtual_machine=virtual_machine)

    def set_local_path(self, local_path: str):
        super(S3ParallelsOperation, self).set_local_path(local_path=local_path)

    @classmethod
    def start(cls, *args, **kwargs) -> None:
        bucket_name = kwargs.pop('backup_name', None)
        if bucket_name is None:
            bucket_name = get_parameter(args, argument_type=Text, argument_index=0)

        o = cls(bucket=bucket_name, **kwargs)
        try:
            o.execute(*args, **kwargs)
        finally:
            del o
