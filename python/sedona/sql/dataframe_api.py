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

import functools
import inspect
import itertools
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Mapping,
    Tuple,
    Type,
    Union,
)
import typing

from pyspark.sql import SparkSession, Column, functions as f


ColumnOrName = Union[Column, str]
ColumnOrNameOrNumber = Union[Column, str, float, int]


def _convert_argument_to_java_column(arg: Any) -> Column:
    if isinstance(arg, Column):
        return arg._jc
    elif isinstance(arg, str):
        return f.col(arg)._jc
    elif isinstance(arg, Iterable):
        return f.array(*[Column(_convert_argument_to_java_column(x)) for x in arg])._jc
    else:
        return f.lit(arg)._jc


def call_sedona_function(object_name: str, function_name: str, args: Union[Any, Tuple[Any]]) -> Column:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active spark session was detected. Unable to call sedona function.")
    
    # apparently a Column is an Iterable so we need to check for it explicitely
    if (not isinstance(args, Iterable)) or isinstance(args, str) or isinstance(args, Column):
        args = [args]

    args = map(_convert_argument_to_java_column, args)

    jobject = getattr(spark._jvm, object_name)
    jfunc = getattr(jobject, function_name)
    
    jc = jfunc(*args)
    return Column(jc)


def _get_type_list(annotated_type: Type) -> Tuple[Type, ...]:
    """Convert a type annotation into a tuple of types.
    For most types this will be a tuple with a single element, but
    for Union a tuple with multiple elements will be returned.

    :param annotated_type: Type annotation to convert a tuple.
    :type annotated_type: Type
    :return: Tuple of all types covered by the type annotation.
    :rtype: Tuple[Type, ...]
    """
    # in 3.8 there is a much nicer way to do this with typing.get_origin
    # we have to be a bit messy until we drop support for 3.7
    if isinstance(annotated_type, typing._GenericAlias) and annotated_type.__origin__._name == "Union":
        # again, there is a really nice method for this in 3.8: typing.get_args 
        valid_types = annotated_type.__args__
    else:
        valid_types = (annotated_type,)
    
    return valid_types


def _strip_extra_from_class_name(class_name):
    return class_name[len("<class '"):-len("'>")].split(".")[-1]


def _get_readable_name_for_type(type: Type) -> str:
    """Get a human readable name for a type annotation used on a function's parameter.

    :param type: Type annotation for a parameter.
    :type type: Type
    :return: Human readable name for the type annotation.
    :rtype: str
    """
    if isinstance(type, typing._GenericAlias) and type.__origin__._name == "Union":
        return f"Union[{', '.join((_strip_extra_from_class_name(str(x)) for x in type.__args__))}]"
    return _strip_extra_from_class_name(str(type))


def _get_bound_arguments(f: Callable, *args, **kwargs) -> Mapping[str, Any]:
    """Bind the passed arguments to f with actual parameter names, including defaults.

    :param f: Function to bind arguments for.
    :type f: Callable
    :return: Dictionary of parameter names to argument values.
    :rtype: Mapping[str, Any]
    """
    f_signature = inspect.signature(f)
    bound_args = f_signature.bind(*args, **kwargs)
    bound_args.apply_defaults()
    return bound_args


def _check_bound_arguments(bound_args: Mapping[str, Any], type_annotations: List[Type], function_name: str) -> None:
    """Check bound arguments against type annotations and raise a ValueError if any do not match.

    :param bound_args: Bound arguments to check.
    :type bound_args: Mapping[str, Any]
    :param type_annotations: Type annotations to check bound_args against.
    :type type_annotations: List[Type]
    :param function_name: Name of the function that is being checked for, used in the exception if raised.
    :type function_name: str
    :raises ValueError: If a bound argument does not match the parameter type.
    """
    for bound_arg_name, bound_arg_value in bound_args.arguments.items():
        annotated_type = type_annotations[bound_arg_name]
        valid_type_list = _get_type_list(annotated_type)
        if not any([isinstance(bound_arg_value, valid_type) for valid_type in valid_type_list]):
            raise ValueError(f"Incorrect argument type: {bound_arg_name} for {function_name} should be {_get_readable_name_for_type(annotated_type)} but received {_strip_extra_from_class_name(str(type(bound_arg_value)))}.")


def validate_argument_types(f: Callable) -> Callable:
    """Validates types of arguments passed to a dataframe API style function.
    Arguments will need to be either strings, columns, or match the typehints of f.
    This function is meant to be used a decorator.

    :param f: Function to validate for.
    :type f: Callable
    :return: f wrapped with type validation checks.
    :rtype: Callable
    """
    def validated_function(*args, **kwargs) -> Column:
        # all arguments are Columns or strings are always legal, so only check types when one of the arguments is not a column
        if not all([isinstance(x, Column) or isinstance(x, str) for x in itertools.chain(args, kwargs.values())]):
            bound_args = _get_bound_arguments(f, *args, **kwargs)
            type_annotations = typing.get_type_hints(f)
            _check_bound_arguments(bound_args, type_annotations, f.__name__)

        return f(*args, **kwargs)
    return functools.update_wrapper(validated_function, f)
