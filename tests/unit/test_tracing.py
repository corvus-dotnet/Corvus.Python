from concurrent.futures import ThreadPoolExecutor
import contextvars

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from corvus_python.monitoring import in_current_context

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="none")


def _current_span_id(_: int) -> int:
    return trace.get_current_span().get_span_context().span_id


def _span(span_id: int) -> NonRecordingSpan:
    return NonRecordingSpan(SpanContext(trace_id=1, span_id=span_id, is_remote=False, trace_flags=TraceFlags(1)))


def test_in_current_context_runs_work_on_a_thread_pool_in_the_callers_current_span():
    with trace.use_span(_span(42)), ThreadPoolExecutor(max_workers=4) as pool:
        span_ids = list(pool.map(in_current_context(_current_span_id), range(20)))

    assert span_ids == [42] * 20


def test_without_in_current_context_work_on_a_thread_pool_has_no_current_span():
    with trace.use_span(_span(42)), ThreadPoolExecutor(max_workers=1) as pool:
        span_ids = list(pool.map(_current_span_id, range(1)))

    assert span_ids == [trace.INVALID_SPAN.get_span_context().span_id]


def test_in_current_context_uses_the_context_it_was_wrapped_in():
    request_id.set("first")
    wrapped = in_current_context(request_id.get)
    request_id.set("second")

    with ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(wrapped).result() == "first"


def test_in_current_context_gives_each_call_its_own_copy_of_the_context():
    def set_and_get(value: str) -> str:
        request_id.set(value)
        return request_id.get()

    request_id.set("caller")
    wrapped = in_current_context(set_and_get)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(wrapped, ["a", "b", "c", "d"])) == ["a", "b", "c", "d"]
        assert pool.submit(in_current_context(request_id.get)).result() == "caller"
    assert request_id.get() == "caller"


def test_in_current_context_keeps_the_functions_name_and_return_value():
    def add(a: int, b: int) -> int:
        return a + b

    wrapped = in_current_context(add)

    assert wrapped.__name__ == "add"
    assert wrapped(1, b=2) == 3
