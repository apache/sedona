#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import inspect
import types

from sedona.exceptions import InvalidParametersException

from typing import Any

try:
    from typing import GenericMeta
except ImportError:
    class GenericMeta(type):
        pass


def is_subclass_with_typing(type_a: Any, type_b: Any):
    if isinstance(type_a, GenericMeta) and isinstance(type_b, GenericMeta):
        return type_a == type_b
    elif isinstance(type_a, GenericMeta) and not isinstance(type_b, GenericMeta):
        python_type_a = type_a.__orig_bases__[0]
        return issubclass(python_type_a, type_b)
    elif not isinstance(type_a, GenericMeta) and isinstance(type_b, GenericMeta):
        python_type_b = type_b.__orig_bases__[0]
        return issubclass(type_a, python_type_b)
    else:
        return issubclass(type_a, type_b)


class MultiMethod:
    """
    Represents a single multimethod.
    """

    def __init__(self, name):
        self._methods = {}
        self.__name__ = name
        self._is_static = False

    def register(self, meth):
        """
        Register a new method as a multimethod
        :param meth:
        :return:
        """
        if str(meth).startswith("<classmethod") or str(meth).startswith("<staticmethod"):
            sig = inspect.signature(meth.__get__(self))
            self._is_static = True
        else:
            sig = inspect.signature(meth)

        # Build a type-signature from the method's annotations
        types = []
        for name, parm in sig.parameters.items():
            if name == 'self':
                continue
            if name == "args" or name == "kwargs":
                raise InvalidParametersException("Can not be used with args and kwargs")

            if name == 'cls':
                continue

            if parm.annotation is inspect.Parameter.empty:
                raise InvalidParametersException(
                    'Argument {} must be annotated with a type'.format(name)
                )
            if parm.default is not inspect.Parameter.empty:
                self._methods[tuple(types)] = meth
            types.append((name, parm.annotation))

        self._methods[tuple(types)] = meth

    def __call__(self, *args, **kwargs):
        """
        Call a method based on type signature of the arguments
        :param args:
        :param kwargs:
        :return:
        """
        if self._is_static:
            types_from_args = tuple(type(arg) for arg in args)
        else:
            types_from_args = tuple(type(arg) for arg in args[1:])

        number_of_arguments = len(types_from_args)
        number_of_kwargs = len(kwargs)

        methods_shortened_to_args = [
            [tuple(tp[1] for tp in types[:number_of_arguments]), types, method]
            for types, method in self._methods.items()
        ]
        methods_which_are_correct = []

        for function_methods in methods_shortened_to_args:
            is_instances = all([
                is_subclass_with_typing(from_args, from_definition)
                for from_args, from_definition in zip(types_from_args, function_methods[0])
            ])

            if is_instances:
                methods_which_are_correct.append(function_methods[1:])

        if methods_which_are_correct:
            for correct_params, method in methods_which_are_correct:
                if len(correct_params) != number_of_arguments + number_of_kwargs:
                    continue
                else:
                    for name, param in correct_params[number_of_arguments:]:
                        try:
                            value = type(kwargs[name])
                        except KeyError:
                            break
                        if is_subclass_with_typing(value, param):
                            pass
                        else:
                            break
                    else:
                        if self._is_static:
                            return method.__get__(self).__call__(*args, **kwargs)
                        return method(*args, **kwargs)

            raise InvalidParametersException("No matching method for given types found")

        else:
            raise InvalidParametersException("No matching method for given types found")

    def __get__(self, instance, cls):
        """
        Descriptor method needed to make calls work in a class
        :param instance:
        :param cls:
        :return:
        """
        if instance is not None:
            return types.MethodType(self, instance)
        else:
            return self


class MultiDict(dict):
    """
    Special dictionary to build multimethods in a metaclass
    """

    def __setitem__(self, key, value):
        if key in self:
            # If key already exists, it must be a multimethod or callable
            current_value = self[key]
            if isinstance(current_value, MultiMethod):
                current_value.register(value)
            else:
                mvalue = MultiMethod(key)
                mvalue.register(current_value)
                mvalue.register(value)
                super().__setitem__(key, mvalue)
        else:
            super().__setitem__(key, value)


class MultipleMeta(type):
    """
    Metaclass that allows multiple dispatch of methods
    """

    def __new__(cls, clsname, bases, clsdict):
        return type.__new__(cls, clsname, bases, dict(clsdict))

    @classmethod
    def __prepare__(cls, clsname, bases):
        return MultiDict()
