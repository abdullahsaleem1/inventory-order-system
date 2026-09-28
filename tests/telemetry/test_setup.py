"""
Week 10 — TracerProvider configuration.

Covers the parts of the bootstrap that are pure functions of settings: the
Resource that tells Jaeger which service a span came from, sampler selection,
exporter resolution, and the idempotency of `init_tracing`.
"""
import os
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    TraceIdRatioBased,
)

from src.core.config import get_settings
from src.core.telemetry.setup import (
    _resolve_service_name,
    build_exporter,
    build_resource,
    build_sampler,
    is_tracing_active,
    shutdown_tracing,
)


def _describe(sampler) -> str:
    """Structural identity for samplers.

    `Sampler` subclasses do not implement `__eq__`, and `ParentBased` is
    constructed fresh on every call, so identity comparison is useless here.
    """
    if isinstance(sampler, ParentBased):
        return f"parentbased({_describe(sampler._root)})"
    if isinstance(sampler, TraceIdRatioBased):
        return f"ratio({sampler.rate})"
    return sampler.get_description()


@pytest.fixture
def expected_names():
    """The full set of supported sampler names, per OTel's own vocabulary."""
    return {
        "always_on",
        "always_off",
        "traceidratio",
        "parentbased_always_on",
        "parentbased_always_off",
        "parentbased_traceidratio",
    }


class TestResource:
    def test_service_identity_is_exposed_for_jaeger(self) -> None:
        resource = build_resource("inventory-worker", "0.7.0")

        assert resource.attributes["service.name"] == "inventory-worker"
        assert resource.attributes["service.version"] == "0.7.0"
        assert resource.attributes["deployment.environment.name"] == get_settings().ENV

    def test_instance_id_distinguishes_replicas(self) -> None:
        """`docker compose up --scale worker=3` must show three nodes, not one."""
        first = build_resource("inventory-worker", "0.7.0").attributes["service.instance.id"]
        second = build_resource("inventory-worker", "0.7.0").attributes["service.instance.id"]

        assert first == second, "same process must keep a stable instance id"
        assert first.startswith("inventory-worker-")
        assert str(os.getpid()) in first

    def test_resource_attributes_env_override_defaults(self) -> None:
        with patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "app.tier=backend"}):
            resource = build_resource("api", "0.7.0")
        assert resource.attributes["app.tier"] == "backend"


class TestSampler:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("always_on", _describe(ALWAYS_ON)),
            ("always_off", _describe(ALWAYS_OFF)),
            ("parentbased_always_on", _describe(ParentBased(ALWAYS_ON))),
            ("parentbased_always_off", _describe(ParentBased(ALWAYS_OFF))),
        ],
    )
    def test_named_samplers(self, name, expected) -> None:
        assert _describe(build_sampler(name, "1.0")) == expected

    @pytest.mark.parametrize("name", ["ALWAYS_ON", "  Always_Off  ", "parentbased_Always_On"])
    def test_sampler_name_is_case_and_whitespace_insensitive(self, name) -> None:
        """`OTEL_TRACES_SAMPLER=ParentBased_Always_On` is a common typo."""
        assert _describe(build_sampler(name, "1.0")).startswith(
            _describe(build_sampler(name.strip().lower(), "1.0"))
        )

    def test_every_documented_sampler_name_is_supported(self, expected_names) -> None:
        for name in expected_names:
            assert _describe(build_sampler(name, "1.0"))

    def test_traceidratio_uses_the_arg(self) -> None:
        sampler = build_sampler("traceidratio", "0.25")
        assert isinstance(sampler, TraceIdRatioBased)
        assert sampler.rate == 0.25

    def test_parentbased_ratio_wraps_the_ratio_sampler(self) -> None:
        """Parent-based so a worker never drops a trace the API already sampled."""
        sampler = build_sampler("parentbased_traceidratio", "0.5")
        assert isinstance(sampler, ParentBased)
        assert isinstance(sampler._root, TraceIdRatioBased)
        assert sampler._root.rate == 0.5

    @pytest.mark.parametrize("raw", ["not-a-number", "", None, "  "])
    def test_unparseable_ratio_falls_back_to_always_on(self, raw) -> None:
        assert _describe(build_sampler("parentbased_traceidratio", raw)) == _describe(
            ParentBased(ALWAYS_ON)
        )
        assert _describe(build_sampler("traceidratio", raw)) == _describe(ALWAYS_ON)

    @pytest.mark.parametrize("raw", ["-1.0", "2.0", "99"])
    def test_out_of_range_ratio_is_clamped(self, raw) -> None:
        sampler = build_sampler("traceidratio", raw)
        assert 0.0 <= sampler.rate <= 1.0

    def test_zero_ratio_keeps_itself_zero(self) -> None:
        """`OTEL_TRACES_SAMPLER_ARG=0` is a deliberate "sample nothing", not a
        typo to be corrected."""
        assert build_sampler("traceidratio", "0").rate == 0.0
        assert build_sampler("traceidratio", "0.0").rate == 0.0

    def test_unknown_name_falls_back_to_parentbased_always_on(self) -> None:
        assert _describe(build_sampler("nonsense", "1.0")) == _describe(ParentBased(ALWAYS_ON))

    def test_empty_name_falls_back_to_parentbased_always_on(self) -> None:
        assert _describe(build_sampler("", "1.0")) == _describe(ParentBased(ALWAYS_ON))


class TestExporterResolution:
    def test_no_endpoint_and_no_console_means_no_exporter(self) -> None:
        """Supported mode: a real TracerProvider with context propagation, but
        spans are dropped instead of sent anywhere. Keeps the test suite quiet."""
        settings = get_settings()
        with patch.multiple(
            settings, OTEL_EXPORTER_OTLP_ENDPOINT=None, OTEL_CONSOLE_EXPORTER=False
        ):
            exporter, protocol = build_exporter()

        assert exporter is None
        assert protocol == "none"

    def test_console_exporter_takes_priority_over_the_endpoint(self) -> None:
        settings = get_settings()
        with patch.multiple(
            settings,
            OTEL_CONSOLE_EXPORTER=True,
            OTEL_EXPORTER_OTLP_ENDPOINT="http://jaeger:4317",
        ):
            exporter, protocol = build_exporter()

        assert protocol == "console"
        assert type(exporter).__name__ == "ConsoleSpanExporter"

    def test_grpc_endpoint(self) -> None:
        settings = get_settings()
        with patch.multiple(
            settings,
            OTEL_CONSOLE_EXPORTER=False,
            OTEL_EXPORTER_OTLP_ENDPOINT="http://jaeger:4317",
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
        ):
            exporter, protocol = build_exporter()

        assert protocol == "grpc"
        assert type(exporter).__name__ == "OTLPSpanExporter"

    def test_http_endpoint_gets_the_traces_path_appended(self) -> None:
        """The single most common OTLP misconfiguration: pointing at :4318
        without /v1/traces. Be forgiving rather than dropping every span."""
        settings = get_settings()
        with patch.multiple(
            settings,
            OTEL_CONSOLE_EXPORTER=False,
            OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318",
            OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf",
        ):
            _, protocol = build_exporter()
        assert protocol == "http/protobuf"

    def test_unknown_protocol_falls_back_to_grpc(self) -> None:
        settings = get_settings()
        with patch.multiple(
            settings,
            OTEL_CONSOLE_EXPORTER=False,
            OTEL_EXPORTER_OTLP_ENDPOINT="http://jaeger:4317",
            OTEL_EXPORTER_OTLP_PROTOCOL="carrier-pigeon",
        ):
            _, protocol = build_exporter()
        assert protocol == "grpc"


class TestServiceNameResolution:
    def test_env_var_wins_over_the_code_default(self) -> None:
        """Lets docker-compose relabel a deployment without a code change."""
        with patch.dict(os.environ, {"OTEL_SERVICE_NAME": "renamed-by-compose"}):
            assert _resolve_service_name("inventory-worker") == "renamed-by-compose"

    def test_code_default_used_when_env_is_absent(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert _resolve_service_name("inventory-worker") == "inventory-worker"

    def test_blank_env_falls_through_to_the_code_default(self) -> None:
        with patch.dict(os.environ, {"OTEL_SERVICE_NAME": "  "}):
            assert _resolve_service_name("inventory-worker") == "inventory-worker"

    def test_settings_default_is_the_last_resort(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert _resolve_service_name(None) == get_settings().OTEL_SERVICE_NAME


class TestLifecycle:
    def test_tracing_is_active_in_the_test_process(self) -> None:
        """`src.main` boots the provider on import, so the rest of the suite
        runs with a real provider and real (but unexported) spans."""
        assert is_tracing_active()

    def test_shutdown_is_idempotent(self) -> None:
        """Called from both the app's shutdown hook and atexit."""
        shutdown_tracing()
        shutdown_tracing()

    def test_reinit_after_shutdown_works(self) -> None:
        from src.core.telemetry.setup import init_tracing

        init_tracing("after-restart", force=True)
        assert is_tracing_active()
        # Leave the process-wide provider in place for the rest of the suite.
        init_tracing(force=True)
        assert is_tracing_active()

    def test_init_is_idempotent_while_active(self) -> None:
        from src.core.telemetry.setup import init_tracing

        first = init_tracing("a")
        second = init_tracing("b")
        assert first is not None and second is not None
        assert is_tracing_active()

    def test_disabled_tracing_installs_nothing(self) -> None:
        from src.core.telemetry.setup import init_tracing

        settings = get_settings()
        with patch.multiple(settings, OTEL_ENABLED=False):
            assert init_tracing("ignored", force=True) is None
        # Restore a working provider for the rest of the suite.
        init_tracing(force=True)
        assert is_tracing_active()
