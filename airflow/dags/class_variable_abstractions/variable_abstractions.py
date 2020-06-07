"""
variable_abstractions.py
~~~~~~~~~~~~~~~~~~~~~~~~
TODO: doc string
TODO: Talk to team to get a better understanding of what should go into each of the class component methods

TODO: These variables should be passed into the ORMs that will be dynamically created
TODO: Have a list of generated variables that will be auto-created upon ORM creation


Good, no import issues
"""
import os
import pathlib
from abc import ABCMeta, abstractmethod
from typing import Dict, Any
from datetime import datetime

# Data Class Components #
####################################################################################################
class DataClassComponentsInterface(metaclass=ABCMeta):
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
                        hasattr(subclass, 'data_source')
                        and callable(subclass.set_data_source)
                )
                and
                (
                        hasattr(subclass, 'data_destination')
                        and callable(subclass.set_data_destination)
                )
                and
                (
                        hasattr(subclass, 'description')
                        and callable(subclass.set_description)
                )
                and
                (
                        hasattr(subclass, 'query_logic')
                        and callable(subclass.set_query_logic)
                )
                and
                (
                        hasattr(subclass, 'name')
                        and callable(subclass.set_name)
                )
                and
                (
                        hasattr(subclass, 'variable_type')
                        and callable(subclass.set_variable_type)
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
    def set_data_source(self, data_source: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_data_destination(self, destination: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_description(self, description: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_query_logic(self, query_logic: str, from_file=False, query_=None) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_name(self, name: str) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_data_provenance(self, data_provenance: str) -> None:
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
    def set_provide_context(self, provide_context: Dict[str, bool]) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_op_kwargs(self, op_kwargs_namespace: Dict[str, Dict[str, Any]]) -> None:
        """Setter Method"""
        raise NotImplementedError

    @abstractmethod
    def set_dag_id(self, dag_id: str) -> None:
        """Setter Method"""
        raise NotImplementedError




class DataClassComponents(DataClassComponentsInterface):
    """Concrete class for defining and creating data class components

    """
    __slots__ = ['data_source', 'data_destination', 'description',
                 'query_logic', 'name', 'data_provenance', 'data_quality',
                 'variable_type', 'schedule', 'start_date',
                 'provide_context', 'op_kwargs_namespace', 'dag_id']

    def __init__(self):
        super(DataClassComponentsInterface).__init__()
        self.data_source: str = ''
        self.data_destination: str = ''
        self.description: str = ''
        self.query_logic: str = ''
        self.name: str = ''
        self.data_provenance: str = ''
        self.data_quality: str = ''
        self.variable_type: str = ''
        self.schedule: str = '@daily'
        self.start_date: datetime = None
        self.provide_context: Dict[str, bool] = {}
        self.op_kwargs: Dict[str, Dict[str, Any]] = {}
        self.dag_id: str = ''

    def __setattr__(self, key, value) -> None:
        """Python internal method"""
        # Add te new attribute to the current class
        self.__dict__[key] = value
        # Add to __slots__
        self.__slots__.append(key)
        # Add to the base class that this class inherits from
        super().__dict__[key] = value

    def set_data_source(self, data_source: str) -> None:
        self.data_source = data_source

    def set_data_destination(self, destination: str) -> None:
        self.data_destination = destination

    def set_description(self, description: str) -> None:
        self.description = description

    def set_query_logic(self, query_logic: str, from_file=False, query_=None) -> None:
        # Insert the dates list into the query
        if from_file and query_:
            query_path = os.path.join(pathlib.Path(__file__).parent.parent,
                                      'stored_queries', query_+'.sql')
            with open(query_path, 'r') as f:
                self.query_logic = f.read()
        else:
            self.query_logic = query_logic

    def set_data_provenance(self, data_provenance: str) -> None:
        self.data_provenance = data_provenance

    def set_name(self, name: str) -> None:
        self.name = name

    def set_data_quality(self, data_quality: str) -> None:
        self.data_quality = data_quality

    def set_variable_type(self, variable_type: str) -> None:
        self.variable_type = variable_type

    def set_schedule(self, schedule: str) -> None:
        self.schedule = schedule

    def set_start_date(self, start_date: datetime) -> None:
        self.start_date = start_date

    def set_provide_context(self, provide_context: Dict[str, bool]) -> None:
        self.provide_context = provide_context

    def set_op_kwargs(self, op_kwargs: Dict[str, Dict[str, Any]]) -> None:
        self.op_kwargs = op_kwargs

    def set_dag_id(self, dag_id: str) -> None:
        self.dag_id=dag_id


