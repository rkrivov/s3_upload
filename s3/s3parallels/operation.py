#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any, Text
from uuid import UUID

import pytz

from s3.s3base.s3baseobject import S3Base, get_name_hash, get_file_info, INFO_LOCAL, INFO_OP, OP_INSERT, \
    INFO_FIELD_NAME, \
    INFO_FIELD_SIZE, INFO_FIELD_MTIME, INFO_FIELD_HASH, OP_DELETE, INFO_REMOTE
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
from utils.convertors import append_start_path_sep, append_end_path_sep, \
    remove_start_path_sep, convert_value_to_type, \
    decode_string, encode_string, remove_end_path_sep, time_to_short_string
from utils.dbase import SQLBuilder
from utils.functions import print_progress_bar, get_parameter

logger = get_logger(__name__)


def get_virtual_machine_type(virtual_machine: ParallelsVirtualMachine) -> Optional[str]:
    virtual_machine_home = virtual_machine.get('home')

    virtual_machine_home = remove_end_path_sep(virtual_machine_home)
    _, virtual_machine_type = os.path.splitext(virtual_machine_home)

    if len(virtual_machine_type) > 0:
        virtual_machine_type = virtual_machine_type[1:].upper()

        return virtual_machine_type

    return None


class S3ParallelsOperation(S3Base):
    DATABASE = os.path.join(consts.WORK_FOLDER, 'cache/parallels.cache')
    TABLE = "virtual_machines"

    FIELD_ID = 'id'
    FIELD_UUID = 'uuid'
    FIELD_CONFIG = 'config'

    SCRIPT = f"""
    --- The script was written by Roman N. Krivov a.k.a. Eochaid Bres Drow
    --- on {datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S %Z')} 

    --- Create table {TABLE} if it doesn't exists
    CREATE TABLE IF NOT EXISTS {TABLE} (
        {FIELD_ID} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        {FIELD_UUID} TEXT NOT NULL,
        {FIELD_CONFIG} TEXT);

    --- Create indexes for table {TABLE}
    CREATE UNIQUE INDEX IF NOT EXISTS {TABLE}_{FIELD_UUID}_idx ON {TABLE} ({FIELD_UUID});
    """

    def __init__(self, bucket: str, **kwargs):
        self._database = dbase.create_database(name=S3ParallelsOperation.DATABASE)
        self._database.execute_script(S3ParallelsOperation.SCRIPT)

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

        if self._need_archive and self._need_pack:
            raise Exception(f"{type(self).__name__} initialize failure: you need to use either packing or archiving")

        super(S3ParallelsOperation, self).__init__(local_path=self.parallels.parallels_home_path, bucket=bucket,
                                                   remote_path='Parallels/')

    def __del__(self):
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

    def get_virtual_machine_info(self, virtual_machine_id: VirtualMachineID) -> Optional[Dict[str, Any]]:
        vm_info = self.load_virtual_machine_info(virtual_machine_id=virtual_machine_id)

        if vm_info is None:
            try:
                vm_info = self.parallels.get_virtual_machine(virtual_machine_id=virtual_machine_id)
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

                        name = local_file_path
                        if name.startswith(local_path):
                            name = name[len(local_path):]
                            name = append_start_path_sep(name)

                        name_hash = get_name_hash(name)

                        file_info = get_file_info(file=local_file_path)

                        operation_info = operations.setdefault(name_hash, {})
                        operation_info[INFO_LOCAL] = file_info
                        operation_info[INFO_OP] = OP_INSERT

        return operations

    def _fetch_remote_files_list(self, remote_path: str = None,
                                 operations_list: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        if remote_path is None:
            remote_path = self._archive_path

        if operations_list is None:
            operations_list = {}

        prefix = append_end_path_sep(remote_path)
        objects_count = self.storage.get_objects_count(prefix=prefix)

        if objects_count > 0:
            loaded_objects_count = 0

            for remote_object in self.storage.fetch_bucket_objects(prefix=prefix):
                if self.show_progress:
                    loaded_objects_count += 1
                    print_progress_bar(iteration=loaded_objects_count,
                                       total=objects_count,
                                       prefix='Fetching remote objects.py')

                file_name = remote_object.get('Key')

                name = file_name
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    name = append_start_path_sep(name)

                name_hash = get_name_hash(name)

                file_size = convert_value_to_type(remote_object.get('Size', None), to_type=int)
                file_mtime = convert_value_to_type(remote_object.get('LastModified', None),
                                                   to_type=datetime)
                file_mtime = file_mtime.replace(tzinfo=pytz.UTC)
                file_mtime = datetime.fromtimestamp(time.mktime(file_mtime.timetuple()))
                file_hash = convert_value_to_type(remote_object.get('ETag', None), to_type=str)

                if file_hash is not None:
                    file_hash = remote_brackets(file_hash, '"')

                file_info = {
                    INFO_FIELD_NAME: file_name,
                    INFO_FIELD_SIZE: file_size,
                    INFO_FIELD_MTIME: file_mtime,
                    INFO_FIELD_HASH: file_hash
                }

                operation_info = operations_list.setdefault(name_hash, {})

                operation_info[INFO_REMOTE] = file_info
                operation_info[INFO_OP] = OP_DELETE

        return operations_list

    def _init_parallels_object(self) -> Parallels:
        self._parallels = Parallels()
        return self._parallels

    def load_virtual_machine_info(self, virtual_machine_id: VirtualMachineID) -> Optional[Dict[str, Any]]:
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

    def store_virtual_machine_info(self, virtual_machine: ParallelsVirtualMachine) -> None:
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

        virtual_machine_remote_path = os.path.join(
            'Parallels/',
            remove_start_path_sep(virtual_machine_remote_path)
        )

        virtual_machine_remote_path = os.path.join(
            append_end_path_sep(virtual_machine_remote_path),
            append_end_path_sep(file_name)
        )

        self.set_local_path(virtual_machine_home)
        self.set_archive_path(virtual_machine_remote_path)

    def _process_with_virtual_machine(self, virtual_machine: ParallelsVirtualMachine) -> None:
        self._init_path(virtual_machine=virtual_machine)

        files = self._fetch_local_files_list(local_path=self._local_path)
        files = self._fetch_remote_files_list(remote_path=self._archive_path, operations_list=files)

        if len(files) > 0:
            files = self._check_files(files)

            if len(files) > 0:
                self.operation(files)

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
                    f" and uptime is {time_to_short_string(virtual_machine_uptime)}."
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

    def process(self):
        virtual_machine_id_list = []

        if self.vm_uuid is not None:
            virtual_machine_id_list.append(self.vm_uuid)

        for virtual_machine in self.parallels.get_virtual_machine_iter(virtual_machine_id_list):
            self._process(virtual_machine=virtual_machine)

    @classmethod
    def start(cls, *args, **kwargs) -> None:
        bucket_name = kwargs.pop('backup_name', None)
        if bucket_name is None:
            bucket_name = get_parameter(args, type=Text, index=0)

        o = cls(bucket=bucket_name, **kwargs)
        try:
            o.execute(*args, **kwargs)
        finally:
            del o
