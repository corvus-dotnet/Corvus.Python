from opentelemetry import trace
from functools import wraps


def start_as_current_span_with_method_name(tracer: trace.Tracer):
    """
        Function decorator which starts a new span with the full name of the method (i.e. class_name.method_name for
        methods within classes, or just method_name for standalone functions) as the span name. The span is then set as
        the current span for the duration of the method call and can be accessed using trace.get_current_span().

        Args:
            tracer (trace.Tracer): The tracer to use for starting the span. Create a tracer for the source file using
            trace.get_tracer(__name__) and pass it to this decorator.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(name=func.__qualname__):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def all_methods_start_new_current_span_with_method_name(tracer: trace.Tracer):
    """
        Class decorator which applies start_as_current_span_with_method_name to all methods within the class.

        Args:
            tracer (trace.Tracer): The tracer to use for starting the span. Create a tracer for the source file using
            trace.get_tracer(__name__) and pass it to this decorator.
    """

    decorator = start_as_current_span_with_method_name(tracer)

    def decorate(cls):
        for attr in cls.__dict__:
            item = getattr(cls, attr)
            if callable(item):
                setattr(cls, attr, decorator(item))

        return cls

    return decorate


def add_attributes_to_span(span: trace.Span, **kwargs: dict[str, any]):
    """
        Adds the specified key-value pairs to the specified span as attributes.

        For example, calling:
            add_attributes_to_span(span, key1="value1", key2="value2")
        is equivalent to calling:
            span.set_attributes({"key1": "value1", "key2": "value2"})

        Args:
            **kwargs: The key-value pairs to add to the span as attributes.
    """
    if span is not None:
        kwargs_as_strings = {k: str(v) for k, v in kwargs.items()}
        span.set_attributes(kwargs_as_strings)


def add_attributes_to_current_span(**kwargs: dict[str, any]):
    """
        Adds the specified key-value pairs to the current span as attributes.

        Args:
            **kwargs: The key-value pairs to add to the span as attributes.
    """
    add_attributes_to_span(trace.get_current_span(), **kwargs)


def add_kwargs_to_span(span: trace.Span, keys: list[str], source_kwargs: dict[str, any]):
    """
        Adds the specified keys from the source_kwargs dictionary to the span as attributes.

        Args:
            span (trace.Span): The span to add the attributes to.
            keys (list[str]): The keys from the source_kwargs to add to the span. These are manually specified to avoid
            adding sensitive information to the span.
            source_kwargs (dict[str, any]): The dictionary to get the values from. This is typically the kwargs
            dictionary of the method being traced.
    """
    kwargs_to_add = {key: source_kwargs[key] for key in keys if key in source_kwargs}
    add_attributes_to_span(span, **kwargs_to_add)


def add_kwargs_to_current_span(keys: list[str], source_kwargs: dict[str, any]):
    """
        Adds the specified keys from the source_kwargs dictionary to the current span as attributes.

        Args:
            keys (list[str]): The keys from the source_kwargs to add to the span. These are manually specified to avoid
            adding sensitive information to the span.
            source_kwargs (dict[str, any]): The dictionary to get the values from. This is typically the kwargs
            dictionary of the method being traced.
    """
    span = trace.get_current_span()
    add_kwargs_to_span(span, keys, source_kwargs)
