"""
table_abstractions.py
~~~~~~~~~~~~~~~~~~~~~

Good, no import issues
"""
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Dict, Any


class TableAbstractionInterface(metaclass=ABCMeta):
    """An interface that builds from DataClassComponentsMeta
    """

    @classmethod
    def __subclasshook__(cls, subclass) -> bool:
        """Checks to see if the sub-class is a true subclass of the base class
        and checks to see if the class instance is derived from the base class.
        NOTE:
            __subclasshook__ consolidates
                __instancecheck__, and __subclasscheck__ into one method.
               :returns: bool
               """
        return (
                (
                        hasattr(subclass, '_name')
                        and callable(subclass.table_name)
                )
                and
                (
                        hasattr(subclass, '_description')
                        and callable(subclass.table_description)
                )
                and
                (
                        hasattr(subclass, '_dialect')
                        and callable(subclass.table_dialect)
                )
                and
                (
                        hasattr(subclass, '_variable_components')
                        and callable(subclass.table_variable_components)
                )
                and
                (
                        hasattr(subclass, '_schedule')
                        and callable(subclass.table_run_schedule)
                )
                and
                (
                        hasattr(subclass, '_depends_on_past')
                        and callable(subclass.table_depends_on_past)
                )
                and
                (
                        hasattr(subclass, 'data_provenance')
                        and callable(subclass.set_data_provenance)
                )
                and
                (
                        hasattr(subclass, 'data_quality')
                        and callable(subclass.set_data_quality)
                )
                and
                (
                        hasattr(subclass, 'schedule')
                        and callable(subclass.set_schedule)
                )
                and
                (
                        hasattr(subclass, 'start_date')
                        and callable(subclass.set_start_date)
                )
                and
                (
                        hasattr(subclass, 'provide_context')
                        and callable(subclass.set_provide_context)
                )
                and
                (
                        hasattr(subclass, 'op_kwargs')
                        and callable(subclass.set_op_kwargs)
                )
                and
                (
                        hasattr(subclass, 'dag_id')
                        and callable(subclass.set_dag_id)
                )
                or
                NotImplemented
        )

    @abstractmethod
    def table_name(self) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def table_description(self) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def table_dialect(self) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def table_variable_components(self) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def table_run_schedule(self) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def table_depends_on_past(self) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_data_quality(self, data_quality: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_variable_type(self, variable_type: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_schedule(self, schedule: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_start_date(self, start_date: datetime) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_provide_context(self, provide_context: bool) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_op_kwargs(self, op_kwargs_namespace: Dict[str, Any]) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_dag_id(self, dag_id: str) -> None:
        """Setter Method"""
        raise NotImplementedError



class TableAbstraction(object):
    """Class for mapping a table"""

    __slots__ = ['table_namespace', '_name', '_description',
                 '_dialect', '_variable_components', '_schedule',
                 '_depends_on_past','provide_context',
                 'op_kwargs', 'dag_id']

    def __init__(self):
        self.table_namespace: Dict[str, Any] = {}
        self._name: str = ''
        self._description: str = ''
        self._dialect: str = ''
        self._variable_components: Dict[str, str] = {}
        self._schedule: str = 'daily'
        self._depends_on_past = False
        self.provide_context: bool = False
        self.op_kwargs: Dict[str, Any] = {}
        self.dag_id: str = ''

    @property
    def table_name(self):
        return self._name

    @table_name.setter
    def table_name(self, name: str):
        self._name = name
        self.table_namespace['name'] = name

    @property
    def table_description(self):
        return self._description

    @table_description.setter
    def table_description(self, description: str):
        self._description = description
        self.table_namespace['description'] = self._description

    @property
    def table_dialect(self):
        return self._dialect

    @table_dialect.setter
    def table_dialect(self, dialect: str):
        self._dialect = dialect
        self.table_namespace['dialect'] = self._dialect

    @property
    def table_variable_components(self):
        return self._variable_components

    @table_variable_components.setter
    def table_variable_components(self, variables: dict):
        self._variable_components = variables
        self.table_namespace['variable_components'] = self._variable_components

    @property
    def table_run_schedule(self):
        return self._schedule

    @table_run_schedule.setter
    def table_run_schedule(self, schedule: str):
        self._schedule = schedule
        self.table_namespace['schedule'] = self._schedule

    @property
    def table_depends_on_past(self):
        return self._depends_on_past

    @table_depends_on_past.setter
    def table_depends_on_past(self, depends: bool):
        self._depends_on_past = depends
        self.table_namespace['depends_on_past'] = self._depends_on_past

    def set_provide_context(self, provide_context: bool) -> None:
        self.provide_context = provide_context
        self.table_namespace['provide_context'] = self.provide_context

    def set_op_kwargs(self, op_kwargs_namespace: Dict[str, Any]) -> None:
        self.op_kwargs = op_kwargs_namespace
        self.table_namespace['op_kwargs'] = self.op_kwargs

    def set_dag_id(self, dag_id: str) -> None:
        self.dag_id = dag_id
        self.table_namespace['dag_id'] = self.dag_id