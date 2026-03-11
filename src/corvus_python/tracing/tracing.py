from functools import wraps
from opentelemetry import trace


def start_as_current_span_with_method_name(tracer: trace.Tracer):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(name=func.__qualname__):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def all_methods_start_new_current_span_with_method_name(tracer: trace.Tracer):
    decorator = start_as_current_span_with_method_name(tracer)
    def decorate(cls):
        for attr in cls.__dict__:
            item = getattr(cls, attr)
            if callable(item):
                setattr(cls, attr, decorator(item))
        return cls
    return decorate


def add_attributes_to_current_span(**kwargs):
    span = trace.get_current_span()
    if span is not None:
        kwargs_as_strings = {k: str(v) for k, v in kwargs.items()}
        span.set_attributes(kwargs_as_strings)
