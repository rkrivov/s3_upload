#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import datetime
import hashlib
import os
import sqlite3
import threading
import time
from datetime import datetime
from sqlite3 import ProgrammingError, Cursor
from typing import Any, Dict, List, Mapping, Optional, Text, Tuple, Type, Union

from utils import consts
from utils.app_logger import get_logger
from utils.arguments import Arguments
from utils.closer import Closer
from utils.convertors import convert_value_to_type, convert_value_to_string, get_string_case
from utils.metasingleton import MetaSingleton
from utils.progress_bar import ProgressBar

logger = get_logger(__name__)


def convert_datetime(val):
    datepart, timepart = val.split(b" ")
    year, month, day = map(int, datepart.split(b"-"))
    timepart_full = timepart.split(b".")
    hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1].decode()))
    else:
        microseconds = 0

    val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)
    return val


def _retry_if_exception(exception):
    return isinstance(exception, Exception) and not isinstance(exception, ProgrammingError)


def _append_string(string: str, value: str, first_string: str, separator: str = ',') -> str:
    if not string:
        string = f"{first_string.strip()} "
    else:
        string += f"{separator.strip()} "

    string += value

    return string


class SQLBuilder(object):
    _name_to_alias: Dict[str, str] = {}
    _alias_to_names: Dict[str, str] = {}
    _select_statement: str = ''
    _from_statement: str = ''
    _where_statement: str = ''
    _limit_statement: str = ''
    _insert_statement: str = ''
    _values_statement: str = ''
    _update_statement: str = ''
    _setting_statement: str = ''
    _delete_statement: str = ''
    _order_by_statmement: str = ''
    _group_by_statement: str = ''
    _having_by_statement: str = ''

    _use_brackets = False
    _use_alias = False

    def __init__(self, use_brackets: bool = False, use_alias: bool = False):
        self._use_brackets = use_brackets
        self._use_alias = use_alias

    def reset(self, use_brackets: bool = False, use_alias: bool = False):
        SQLBuilder._name_to_alias = {}
        SQLBuilder._alias_to_names = {}

        self._select_statement = ''
        self._from_statement = ''
        self._where_statement = ''
        self._limit_statement = ''
        self._insert_statement = ''
        self._values_statement = ''
        self._update_statement = ''
        self._setting_statement = ''
        self._delete_statement = ''
        self._order_by_statmement = ''
        self._group_by_statement = ''
        self._having_by_statement = ''

        self._use_brackets = use_brackets
        self._use_alias = use_alias

    def set_statement_select(self, statement: Union[str, Tuple[str], List[str]], top: int = None,
                             district: bool = False) -> str:
        if isinstance(statement, (list, tuple)):
            for item in statement:
                self.set_statement_select(item)
        else:
            select_statement = 'SELECT'

            if top is not None and top > 0:
                select_statement = f'{select_statement} TOP {top}'

            if district:
                select_statement = f'{select_statement} DISTRICT'

            self._select_statement = _append_string(self._select_statement, statement, select_statement)

        return self._select_statement

    def set_statement_settings(self, statement: Union[str, Tuple[str], List[str]]) -> str:
        if isinstance(statement, (list, tuple)):
            for item in statement:
                self.set_statement_settings(item)
        else:
            self._setting_statement = _append_string(self._setting_statement, statement, 'SET')
        return self._setting_statement

    def set_statement_from(self, statement: Union[str, Tuple[str], List[str]]) -> str:
        if isinstance(statement, (list, tuple)):
            for item in statement:
                return self.set_statement_from(item)
        else:
            self._from_statement = _append_string(self._from_statement, statement, 'FROM')
            return self._from_statement

    def set_statement_where(self, statement: str) -> str:
        self._where_statement = _append_string(self._where_statement, statement, 'WHERE')
        return self._where_statement

    def set_statement_delete(self, statement: str) -> str:
        self._delete_statement = _append_string(self._delete_statement, statement, 'DELETE FROM')
        return self._delete_statement

    def set_statement_update(self, statement: str) -> str:
        self._update_statement = _append_string(self._update_statement, statement, 'UPDATE')
        return self._update_statement

    def set_statmement_order_by(self, statement: str, is_asc: bool = True) -> str:
        if is_asc:
            value_asc = "ASC"
        else:
            value_asc = "DESC"

        self._order_by_statmement = _append_string(self._order_by_statmement, f"{statement} {value_asc}",
                                                   'ORDER BY')
        return self._order_by_statmement

    def set_statmement_group_by(self, statement: str) -> str:
        self._group_by_statement = _append_string(self._group_by_statement, statement, 'GROUP BY')
        return self._group_by_statement

    def set_statement_having(self, statement: str) -> str:
        self._having_by_statement = _append_string(self._having_by_statement, statement, 'HAVING')
        return self._having_by_statement

    def set_statement_insert(self, table_name: str, statement: str) -> str:
        self._insert_statement = f"INSERT INTO {table_name} ({statement})"
        return self._insert_statement

    def set_statement_values(self, statement: str) -> str:
        self._values_statement = f"VALUES ({statement})"
        return self._values_statement

    def set_statmenet_limit(self, limit: int, offset: int = None) -> str:
        self._limit_statement = f'LIMIT {limit}'
        if offset is not None and offset:
            self._limit_statement = f'{self._limit_statement} OFFSET {offset}'
        return self._limit_statement

    def make_select_statement(self):
        value = self._select_statement
        value += ' ' + self._from_statement

        if self._where_statement:
            value += ' ' + self._where_statement

        if self._order_by_statmement:
            value += ' ' + self._order_by_statmement

        if self._group_by_statement:
            value += ' ' + self._group_by_statement

        if self._having_by_statement:
            value += ' ' + self._having_by_statement

        if self._limit_statement:
            value += ' ' + self._limit_statement

        return value

    def make_insert_statement(self):
        return self._insert_statement + ' ' + self._values_statement

    def make_delete_statement(self):
        value = self._delete_statement

        if self._where_statement:
            value += ' ' + self._where_statement

        if self._order_by_statmement:
            value += ' ' + self._order_by_statmement

        if self._limit_statement:
            value += ' ' + self._limit_statement

        return value

    def make_update_statement(self):
        value = self._update_statement + ' ' + self._setting_statement

        if self._where_statement:
            value += ' ' + self._where_statement

        return value

    @staticmethod
    def operator(value1: str, operator: str, value2: str, use_brackets: bool = False) -> str:
        if value2:
            value = f"{value1} {operator.upper()} {value2}"
        else:
            value = f"{operator.upper()} ({value1})"
        if use_brackets:
            value = f"({value})"
        return value

    @staticmethod
    def operator_and(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, 'and', value2, use_brackets=use_brackets)

    @staticmethod
    def operators_and(values: Union[List[str], Tuple[str]]) -> str:
        return " AND ".join(values)

    @staticmethod
    def operator_or(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, 'or', value2, use_brackets=use_brackets)

    @staticmethod
    def operators_or(values: Union[List[str], Tuple[str]]) -> str:
        return " OR ".join(values)

    @staticmethod
    def operator_not(value: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value, 'not', '', use_brackets=use_brackets)

    @staticmethod
    def operator_in(value: str, values: Union[List[Any], Tuple[Any]], use_brackets: bool = False) -> str:
        parameters = ', '.join(['?' for _ in values])
        parameters = f"({parameters})"
        result = f"{value} IN {parameters}"

        if use_brackets:
            result = f"({result})"

        return result

    @staticmethod
    def operator_not_in(value: str, values: Union[List[Any], Tuple[Any]], use_brackets: bool = False) -> str:
        parameters = ', '.join(['?' for _ in values])
        parameters = f"({parameters})"
        result = f"{value} IN {parameters}"

        if use_brackets:
            result = f"({result})"

        return result

    @staticmethod
    def operator_is(value_1: str, value_2: str, use_brackets: bool = False) -> str:
        result = f"{value_1} IS {value_2}"

        if use_brackets:
            result = f"({result})"

        return result

    @staticmethod
    def operator_is_not(value_1: str, value_2: str, use_brackets: bool = False) -> str:
        result = f"{value_1} IS NOT {value_2}"

        if use_brackets:
            result = f"({result})"

        return result

    @staticmethod
    def operator_equal(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, '=', value2, use_brackets=use_brackets)

    @staticmethod
    def operator_not_equal(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, '!=', value2, use_brackets=use_brackets)

    @staticmethod
    def operator_less(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, '<', value2, use_brackets=use_brackets)

    @staticmethod
    def operator_great(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, '>', value2, use_brackets=use_brackets)

    @staticmethod
    def operator_equal_or_less(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, '<=', value2, use_brackets=use_brackets)

    @staticmethod
    def operator_equal_or_great(value1: str, value2: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(value1, '>=', value2, use_brackets=use_brackets)

    @staticmethod
    def operator_equal_with_parameter(field: str, use_brackets: bool = False) -> str:
        return SQLBuilder.operator(field, '=', '?', use_brackets=use_brackets)

    @staticmethod
    def operator_between(value1: str, value2: str, value3: str, use_brackets: bool = False) -> str:
        result = f"{value1} BETWEEN {value2} AND {value3}"

        if use_brackets:
            result = f"({result})"

        return result

    @staticmethod
    def convert_value_to_string(value: Any) -> str:
        if value is None:
            return 'null'

        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, int):
            return f"{value}"
        elif isinstance(value, float):
            return f"{value}"
        elif isinstance(value, bool):
            if value:
                return SQLBuilder.convert_value_to_string(1)
            else:
                return SQLBuilder.convert_value_to_string(0)
        elif isinstance(value, datetime):
            return SQLBuilder.convert_value_to_string(value.isoformat())
        elif isinstance(value, datetime.date):
            return SQLBuilder.convert_value_to_string(value.isoformat())

        return f"{value}"

    @staticmethod
    def value(value: Any) -> str:
        return SQLBuilder.convert_value_to_string(value)

    @staticmethod
    def table(table_name: str, table_alias: str = '', use_brackets: bool = False, use_alias: bool = False) -> str:

        if use_alias:
            if table_alias == '':
                if table_name.upper() in SQLBuilder._name_to_alias:
                    table_alias = SQLBuilder._name_to_alias[table_name.upper()]
                else:
                    table_alias = f"T_{len(SQLBuilder._name_to_alias) + 1}"
                    SQLBuilder._name_to_alias[table_name.upper()] = table_alias
                    SQLBuilder._alias_to_names[table_alias.upper()] = table_name

        if use_brackets:
            if use_alias and table_alias:
                value = f"[{table_name}] AS [{table_alias}]"
            else:
                value = f"[{table_name}]"
        else:
            if use_alias and table_alias is not None:
                value = f"{table_name} AS {table_alias}"
            else:
                value = f"{table_name}"

        return value

    @staticmethod
    def tables(tables_list: Union[List[str], Tuple[str]]) -> str:
        return ", ".join(tables_list)

    @staticmethod
    def field(field_name: str, table_name: str = '', field_alias: str = '', use_brackets: bool = False,
              use_alias: bool = False) -> str:

        if table_name != '':
            if table_name.upper() in SQLBuilder._name_to_alias:
                table_name = SQLBuilder._name_to_alias[table_name.upper()]

        if use_alias:
            if field_alias == '':
                if field_name.upper() in SQLBuilder._name_to_alias:
                    field_alias = SQLBuilder._name_to_alias[field_name.upper()]
                else:
                    field_alias = f"F_{len(SQLBuilder._name_to_alias) + 1}"
                    SQLBuilder._name_to_alias[field_name.upper()] = field_alias
                    SQLBuilder._alias_to_names[field_alias.upper()] = table_name

        if table_name:
            if use_brackets:
                value = f"[{table_name}]."
            else:
                value = f"{table_name}."
        else:
            value = ""

        if use_brackets:
            if use_alias and field_alias:
                value = f"{value}[{field_name}] AS [{field_alias}]"
            else:
                value = f"{value}[{field_name}]"
        else:
            if use_alias and field_alias:
                value = f"{value}{field_name} AS {field_alias}"
            else:
                value = f"{value}{field_name}"
        return value

    @staticmethod
    def fields(fields_list: Union[List[str], Tuple[str]]) -> str:
        return ", ".join(fields_list)

    @staticmethod
    def sql_function(func_name: str, statement: str) -> str:
        return f"{func_name.upper()}({statement})"

    @staticmethod
    def sql_function_count(statement: str) -> str:
        return SQLBuilder.sql_function('count', statement)

    @staticmethod
    def sql_function_sum(statement: str) -> str:
        return SQLBuilder.sql_function('sum', statement)

    @staticmethod
    def sql_function_avg(statement: str) -> str:
        return SQLBuilder.sql_function('avg', statement)


class DBField(object):
    def __init__(self, field_name: str, field_type: Type[Any] = None, field_value: Any = None):

        if field_type is None:
            if field_value is None:
                raise AttributeError(f'The field can\'t have None in the field argument_type and field value')
            if isinstance(field_value, (list, tuple)):
                first_value_type = type(field_value[0])
                for idx in range(len(field_value)):
                    if not isinstance(field_value[idx], first_value_type):
                        raise AttributeError(f'Incorrect {type(field_value[idx]).__name__}. Item {idx} don\'t have '
                                             f'argument_type {first_value_type.__name__}')

            field_type = type(field_value)

        self.__field_name = field_name
        self.__field_type = field_type
        self.__field_value = field_value

    @property
    def value(self):
        return self.__field_value

    @property
    def name(self):
        return self.__field_name

    @property
    def type(self):
        return self.__field_type

    @value.setter
    def value(self, value: Any):
        if not isinstance(value, self.__field_type):
            if isinstance(value, (list, tuple)):
                if len(value) == 0:
                    raise ValueError(f"Incorrent value argument_type. Value has argument_type {type(value).__name__}, "
                                     f"but need {self.__field_type.__name__}")
                for v in value:
                    if not isinstance(v, self.__field_type):
                        raise ValueError(
                            f"Incorrent value argument_type. Value has argument_type {type(v).__name__}, "
                            f"but need {self.__field_type.__name__}")
            else:
                raise ValueError(
                    f"Incorrent value argument_type. Value has argument_type {type(value).__name__}, "
                    f"but need {self.__field_type.__name__}")

        self.__field_value = value


class DBRecord(object):
    def __init__(self, values_map: Mapping[str, Any] = None):
        if values_map is not None:
            for key, value in values_map.items():
                self.__dict__[key] = value

    def __getitem__(self, name: str) -> Any:
        if name in self.__dict__:
            return self.__dict__[name]

        raise ValueError(f"The item with the name [{name}] isn't found")

    def __setitem__(self, name: str, value: Any):
        self.__dict__[name] = DBField(name, type(value), value)

    def __contains__(self, name: str) -> bool:
        if name in self.__dict__:
            return True

        return False

    def __delitem__(self, name: str):
        if name in self.__dict__:
            del self.__dict__[name]

    def __iter__(self):
        return iter(self.__dict__.items())

    def __len__(self):
        return len(self.__dict__)

    def __hash__(self):
        hash_value = hash(str(self))
        return hash_value

    def __missing__(self, name: str):
        raise ValueError(f"Not found item with name [{name}]")

    def __get_values(self):
        return [item for item in self.__dict__.values()]

    def __str__(self):
        values_list = []
        for field in self.__get_values():
            values_list.append(f"{field.name}: {convert_value_to_string(field.value)}")

        ret = ', '.join(values_list)

        return f"{{{ret}}}"

    @property
    def first(self):
        fields_list = self.__get_values()

        return fields_list[0]

    @property
    def last(self):
        fields_list = self.__get_values()

        return fields_list[-1]

    @property
    def tail(self):
        fields_list = self.__get_values()

        return fields_list[1:]

    def put_field(self, *args, **kwargs):
        types_list = kwargs.pop('types', None)

        for argument in args:
            if isinstance(argument, DBField):
                self.__dict__[argument.name] = argument
            elif isinstance(argument, dict):
                for key, value in argument.items():
                    field_type = None

                    if types_list is not None:
                        field_type = types_list.get(key, None)

                    if field_type is None:
                        field_type = type(value) if value is not None else type('')

                    field = DBField(key, field_type=field_type, field_value=value)
                    self.__dict__[field.name] = field
            else:
                raise AttributeError(f"{argument} is not supported")

        for key, value in kwargs.items():
            self.__dict__[key] = value

    def get_value(self, name: str, default: Any = None) -> Any:
        if name in self.__dict__:
            field = self.__dict__[name]
            return field.value

        if default is None:
            raise AttributeError(f"The item with the name [{name}] isn't found")

        return default

    def field(self, name: str):
        if name in self.__dict__:
            field = self.__dict__[name]
            return field

        raise AttributeError(f"The item with the name [{name}] isn't found")


class DBCursor(sqlite3.Cursor):
    pass


class DBConnection(sqlite3.Connection):
    def cursor(self) -> DBCursor:
        return super(DBConnection, self).cursor(DBCursor)


def _convert_name(*args, **kwargs) -> str:
    arguments = Arguments(*args)
    item_name = arguments.get(argument_type=str)
    item_alias = arguments.get(argument_type=str, throw_error=False)

    use_brackets = kwargs.pop('use_brackets', False)
    use_alias = kwargs.pop('use_alias', False)
    is_field = kwargs.pop('is_field', False)

    if use_brackets:
        item_name = f"[{item_name}]"

    if item_alias is not None and item_alias:
        if use_alias:
            item_alias = f"[{item_alias}]"

    if use_alias:
        if is_field:
            item_name = f"{item_name} AS {item_alias}"
        else:
            item_name = f"{item_alias}.{item_name}"

    return item_name


def trace_callback(*args, **kwargs) -> Any:
    for arg in args:
        logger.debug(f'TRACE: {arg}')

    if len(kwargs) > 0:
        logger.debug(vars(kwargs))

    return 0


class DBManager(metaclass=MetaSingleton):
    _fields_info = {}

    TEMPORARY_DATABASE=':memory:'
    ISOLATION_LEVEL_DEFERRED = 'DEFERRED'
    ISOLATION_LEVEL_IMMEDIATE = 'IMMEDIATE'
    ISOLATION_LEVEL_EXCLUSIVE = 'EXCLUSIVE'

    def __init__(self, name: str = TEMPORARY_DATABASE, check_same_thread: bool = False,
                 isolation_level: str = ISOLATION_LEVEL_IMMEDIATE):
        super().__init__()

        self._is_new_database = name.lower() == DBManager.TEMPORARY_DATABASE or not os.path.exists(name)
        self._database_name = name
        self._check_same_thread = check_same_thread
        self._isolation_level = isolation_level
        self._callback_tracebacks_enabled = False

        backup_file_name = self._database_name
        backup_file_path = os.path.dirname(backup_file_name)
        backup_file_name = os.path.basename(backup_file_name)
        backup_file_name, backup_file_type = os.path.splitext(backup_file_name)
        backup_file_datetime = datetime.now()
        backup_file_path = os.path.join(backup_file_path, 'backup')
        backup_file_name += '_'
        backup_file_name += backup_file_datetime.strftime('%Y%m%d')
        backup_file_name += '_'
        backup_file_name += backup_file_datetime.strftime('%H%M%S')
        backup_file_name += backup_file_type

        self._backup_databaseName = os.path.join(backup_file_path, backup_file_name)
        self._is_modified = False

    def __del__(self):
        self.close()

    @property
    def connection(self) -> DBConnection:
        if not hasattr(self, '_connection'):
            self._connection = None

        if self._connection is None:
            self._connection = self.create_connection()

        return self._connection

    @property
    def database_name(self) -> str:
        return self._database_name

    @property
    def callback_tracebacks_enabled(self) -> bool:
        return self._callback_tracebacks_enabled

    @callback_tracebacks_enabled.setter
    def callback_tracebacks_enabled(self, value: bool):
        self._callback_tracebacks_enabled = value
        sqlite3.enable_callback_tracebacks(value)

    @property
    def is_closed(self):
        if hasattr(self, '_connection'):
            return self._connection is None
        else:
            return True

    @property
    def is_new_database(self) -> bool:
        return self._is_new_database

    @property
    def is_modified(self) -> bool:
        return self._is_modified

    def _load_schema(self):
        def get_default_value(for_type: Type[Any]) -> Any:
            if for_type == int:
                return 0
            if for_type == float:
                return 0.00
            if for_type == str:
                return ''
            return None


        def get_type_by_name(type_name: Text) -> Type[Any]:
            if type_name.lower() == 'text':
                return Text
            if type_name.lower() == 'integer':
                return int
            if type_name.lower() == 'real':
                return float
            if type_name.lower() == 'timespamp':
                return float
            return type(None)

        if not hasattr(self, '_fields_info'):
            self._fields_info = {}

        if len(self._fields_info) == 0:
            tables_list = []
            statement = "select NAME from SQLITE_MASTER where TYPE='table' order by NAME"

            try:
                for row in self.connection.execute(statement):
                    tables_list.append(row['name'])
            except Exception as ex:
                logger.error(ex)

            try:
                for table_name in tables_list:
                    statement = "pragma table_info('{}')".format(table_name)
                    for row in self.connection.execute(statement):
                        column_id = row['cid']
                        column_name = row['name']
                        column_type = get_type_by_name(row['type'])
                        column_is_not_null = True if row['notnull'] == 1 else False
                        column_default = row['dflt_value']
                        column_is_primary_key = True if row['pk'] == 1 else False

                        table_info = self._fields_info.setdefault(table_name, {})
                        field_info = table_info.setdefault(column_name, {})

                        if column_default is None:
                            column_default = get_default_value(for_type=column_type)

                        field_info['id'] = column_id
                        field_info['name'] = column_name
                        field_info['type'] = column_type
                        field_info['default'] = column_default

            except Exception as ex:
                logger.error(ex)

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def create_connection(self) -> DBConnection:
        # sqlite3.enable_callback_tracebacks(True)
        self.callback_tracebacks_enabled = True

        if not os.path.exists(os.path.dirname(self._database_name)):
            os.makedirs(os.path.dirname(self._database_name))

        logger.debug(f"Connecting to connection in file {self._database_name}...")

        connection = sqlite3.connect(self._database_name,
                                     check_same_thread=self._check_same_thread,
                                     isolation_level=self._isolation_level,
                                     detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                                     factory=DBConnection)

        md5sum = lambda t: hashlib.md5(t).hexdigest()

        connection.create_function("md5", 1, md5sum)

        connection.set_trace_callback(trace_callback)
        connection.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
        connection.text_factory = lambda x: x.decode(consts.ENCODER)
        # dict_factory
        self._is_closed = False

        return connection

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def close(self):
        if hasattr(self, '_connection'):
            try:
                if self._connection:
                    logger.debug(f"Close connection {os.path.basename(self._database_name)}...")
                    self._connection.close()
                    delattr(self, '_connection')
            except Exception as ex:
                logger.exception(ex)
                raise

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def create_cursor(self) -> DBCursor:
        logger.debug('Create new cursor')
        cursor: DBCursor = self.connection.cursor()
        logger.debug(f'{cursor}')
        return cursor

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def commit(self):
        if self.connection.in_transaction:
            logger.debug(
                f"Commit transaction on connection {os.path.basename(self._database_name)}..."
            )
            self.connection.commit()

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def rollback(self):
        if self._connection is not None:
            logger.debug(f"{self._connection.in_transaction=}")
            if self._connection.in_transaction:
                logger.debug(
                    f"Rollback transaction on connection {os.path.basename(self._database_name)}..."
                )
                self._connection.rollback()

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def prepare_query(self, statement: Text,
                      parameters: Optional[Union[Any, List[Any], Tuple[Any]]] = None) -> DBCursor:
        self._load_schema()

        cursor = self.create_cursor()

        try:
            if cursor is not None:
                if parameters is None:
                    cursor.execute(statement)
                else:
                    parameters = DBManager.convert_parameters(parameters)
                    if DBManager.use_many(parameters):
                        cursor.executemany(statement, parameters)
                    else:
                        cursor.execute(statement, parameters)
        except ProgrammingError as error:
            logger.error(error)

        return cursor

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def execute(self, statement: Text, parameters: Optional[Union[Any, List[Any], Tuple[Any]]] = None) -> Union[
        Any, DBRecord]:
        """
        Execute statement 'statement' with parameters ''parameters'.
        Parameters maybe None or any argument_type.
        The return all the records.
        """
        with Closer(self.prepare_query(statement, parameters)) as cursor:
            for row in cursor.fetchall():
                record = DBManager.convert_row_values(row)
                yield record

        return None

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def execute_once(self, statement: Text, parameters: Optional[Union[Any, List[Any], Tuple[Any]]] = None) -> Optional[
        DBRecord]:
        """
        Execute statement 'statement' with parameters ''parameters'.
        Parameters may be None or any argument_type.
        The return the one record.
        """
        record = None

        with Closer(self.prepare_query(statement, parameters)) as cursor:
            row = cursor.fetchone()
            record = DBManager.convert_row_values(row)

        return record

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def execute_update(self, statement: Text, parameters: Optional[Union[Any, List[Any], Tuple[Any]]] = None) -> int:
        """
        Execute update statement 'statement' with parameters ''parameters'.
        Commit transaction and return True if success. Otherwise rollback transaction end return False.
        Parameters maybe None or any argument_type.
        """

        try:
            with Closer(self.prepare_query(statement, parameters)) as cursor:
                self.commit()

                updated_rows = cursor.rowcount

                if updated_rows > 0:
                    self._is_modified = True
                    logger.debug("{updated_rows} {str}".format(
                        updated_rows=updated_rows,
                        str=get_string_case(updated_rows, 'record was updated', 'records were updated')
                    ))

                return updated_rows
        except sqlite3.Error as error:
            logger.error(f'Exception {type(error).__name__} with message: {str(error)}')
            self.rollback()
            raise error from None

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def execute_script(self, script: Union[bytes, str, Text]) -> bool:
        """
        Run the SQL script to create tables/indexes and other things.
        """
        try:
            with Closer(self.create_cursor()) as cursor:
                cursor.executescript(script)
            return True
        except Exception as ex:
            logger.critical(f"Exception {type(ex).__name__} with message: {ex}")
            raise

    # @retry(retry_on_exception=_retry_if_exception, logger=logger)
    def make_backup(self):
        if not os.path.exists(self._backup_databaseName) and not self.is_new_database:
            db_path = os.path.dirname(self._backup_databaseName)

            if not os.path.exists(db_path):
                os.makedirs(db_path)

            progress = ProgressBar(caption='Backup connection')
            try:
                with sqlite3.connect(self._backup_databaseName) as backup_connection:
                    self._connection.backup(target=backup_connection, progress=progress)
            finally:
                del progress

    @staticmethod
    def convert_parameters(parameters: Any) -> Tuple[Any]:
        if parameters is not None:
            is_many = False
            if isinstance(parameters, list):
                for item in parameters:
                    if isinstance(item, tuple):
                        is_many = True
                        break
            if not isinstance(parameters, tuple):
                if not isinstance(parameters, list):
                    parameters = (parameters,)
                else:
                    if is_many:
                        for ix, item in enumerate(parameters):
                            if not isinstance(item, tuple):
                                item = (item,)
                                parameters[ix] = item
                    else:
                        parameters = tuple(parameters)
        return parameters

    @staticmethod
    def use_many(parameters: Any) -> bool:
        is_many = False
        if isinstance(parameters, list):
            is_many = True
            for item in parameters:
                is_many = isinstance(item, tuple) and is_many
        return is_many

    @classmethod
    def convert_row_values(cls, row: Dict[str, Any]) -> Optional[DBRecord]:
        types = {}
        defaults = {}
        if len(cls._fields_info) > 0:
            for key, _ in row.items():
                for _, fields in cls._fields_info.items():
                    if key in fields:
                        types.setdefault(key, fields[key]['type'])
                        if row[key] is None:
                            row[key] = fields[key]['default']
                        break

        if row is not None:
            record = DBRecord()
            record.put_field(row, types=types)
            logger.debug(str(record))
            return record
        return None


class DBManagerThreaded(DBManager):
    databaseLock = threading.Lock()

    def execute_update(self, statement: Union[bytes, str, Text],
                       parameters: Optional[Union[Any, List[Any], Tuple[Any]]] = None) -> int:
        """
        executeUpdate(statement: Union[Text, bytes][, files_alias: Any][, parameters: Any]) -> Any

        Execute update statement 'statement' with parameters ''parameters'.
        Commit transaction and return True if success. Otherwise rollback transaction end return false.
        Parameters mayby None or any argument_type.
        """

        with self.databaseLock:
            return super(DBManagerThreaded, self).execute_update(statement, parameters)

    def execute_script(self, script: Union[bytes, str, Text]) -> bool:
        """
        Run the SQL script to create tables/indexes and other things.
        """
        with self.databaseLock:
            return super(DBManagerThreaded, self).execute_script(script)


def create_database(name: str) -> DBManager:
    if threading.current_thread().ident != threading.main_thread().ident:
        logger.debug(f"Use DBManagerThreaded({name})")
        return DBManagerThreaded(name=name)
    else:
        logger.debug(f"Use DBManager({name})")
        return DBManager(name=name)


def register_adapters_and_converters():
    def datetime_adapter(value):
        return time.mktime(value.timetuple())

    def datetime_conerter(value):
        if isinstance(value, bytes):
            value = value.decode(consts.ENCODER)
        value = convert_value_to_type(value, to_type=float, default=0)
        return datetime.fromtimestamp(value)

    sqlite3.register_adapter(datetime, datetime_adapter)
    sqlite3.register_converter("timestamp", datetime_conerter)


register_adapters_and_converters()
