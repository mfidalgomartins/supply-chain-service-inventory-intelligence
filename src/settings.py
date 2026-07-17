"""Typed configuration for the analytics, optimization, and operations pipeline."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.config import PROJECT_ROOT

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline.json"


@dataclass(frozen=True)
class AdapterSettings:
    type: str
    source_path: str | None
    erp_path: str | None
    wms_path: str | None
    file_format: str
    column_mapping: dict[str, dict[str, str]]


@dataclass(frozen=True)
class StorageSettings:
    incremental: bool
    compression: str


@dataclass(frozen=True)
class PolicySettings:
    policy_id: str
    safety_stock_factor: float
    cycle_stock_days: int


@dataclass(frozen=True)
class BacktestSettings:
    lookback_days: int
    horizon_days: int
    fold_start_dates: tuple[str, ...]
    annual_holding_cost_rate: float
    ordering_cost: float
    service_tolerance: float
    policies: tuple[PolicySettings, ...]


@dataclass(frozen=True)
class MonteCarloVariant:
    scenario_id: str
    safety_factor_multiplier: float
    cycle_days_delta: int


@dataclass(frozen=True)
class MonteCarloSettings:
    seed: int
    simulations: int
    warmup_days: int
    horizon_days: int
    demand_block_days: int
    target_entities: int
    target_confidence: float
    max_inventory_increase_rate: float
    annual_holding_cost_rate: float
    ordering_cost: float
    variants: tuple[MonteCarloVariant, ...]


@dataclass(frozen=True)
class ActionTrackingSettings:
    events_file: str
    measurement_window_days: int


@dataclass(frozen=True)
class SourceTablePolicy:
    source_system: str
    owner: str
    watermark_column: str | None
    max_lag_hours: int | None
    stale_severity: str


@dataclass(frozen=True)
class SourceGovernanceSettings:
    enabled: bool
    as_of_date: str
    registry_path: str
    table_policies: dict[str, SourceTablePolicy]


@dataclass(frozen=True)
class CausalEvaluationSettings:
    enabled: bool
    seed: int
    permutations: int
    min_units_per_group: int
    pre_days: int
    post_days: int
    alpha: float
    parallel_trend_alpha: float


@dataclass(frozen=True)
class NetworkOptimizationSettings:
    enabled: bool
    horizon_days: int
    demand_lookback_days: int
    annual_holding_cost_rate: float
    shortage_penalty_multiplier: float
    ordering_cost: float
    time_limit_seconds: int
    mip_relative_gap: float


@dataclass(frozen=True)
class ObjectStoreSettings:
    backend: str
    local_root: str
    s3_bucket: str | None
    s3_prefix: str
    s3_endpoint_url: str | None
    s3_region: str
    s3_server_side_encryption: str


@dataclass(frozen=True)
class OrchestrationSettings:
    max_retries: int
    retry_base_seconds: float
    object_store: ObjectStoreSettings


@dataclass(frozen=True)
class PipelineSettings:
    version: int
    adapter: AdapterSettings
    storage: StorageSettings
    backtesting: BacktestSettings
    monte_carlo: MonteCarloSettings
    action_tracking: ActionTrackingSettings
    source_governance: SourceGovernanceSettings
    causal_evaluation: CausalEvaluationSettings
    network_optimization: NetworkOptimizationSettings
    orchestration: OrchestrationSettings


def _positive(value: int | float, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _validate(settings: PipelineSettings) -> None:
    if settings.version != 2:
        raise ValueError(f"Unsupported pipeline config version: {settings.version}")
    if settings.adapter.type not in {"synthetic", "directory", "erp_wms"}:
        raise ValueError("adapter.type must be synthetic, directory, or erp_wms")
    if settings.adapter.file_format not in {"csv", "parquet"}:
        raise ValueError("adapter.file_format must be csv or parquet")
    if settings.adapter.type == "directory" and not settings.adapter.source_path:
        raise ValueError("directory adapter requires source_path")
    if settings.adapter.type == "erp_wms" and not (
        settings.adapter.erp_path and settings.adapter.wms_path
    ):
        raise ValueError("erp_wms adapter requires erp_path and wms_path")
    canonical_tables = {
        "products",
        "suppliers",
        "warehouses",
        "inventory_snapshots",
        "demand_history",
        "purchase_orders",
        "product_classification",
        "intervention_assignments",
        "network_nodes",
        "network_lanes",
        "product_sources",
    }
    unknown_mappings = set(settings.adapter.column_mapping) - canonical_tables
    if unknown_mappings:
        raise ValueError(
            "adapter.column_mapping contains unknown tables: " + ", ".join(sorted(unknown_mappings))
        )
    for table_name, mapping in settings.adapter.column_mapping.items():
        if not isinstance(mapping, dict) or not all(
            isinstance(source, str) and isinstance(target, str)
            for source, target in mapping.items()
        ):
            raise ValueError(f"adapter.column_mapping.{table_name} must map strings to strings")
        if len(set(mapping.values())) != len(mapping):
            raise ValueError(
                f"adapter.column_mapping.{table_name} contains duplicate canonical targets"
            )
    if settings.storage.compression not in {"zstd", "snappy", "gzip", "uncompressed"}:
        raise ValueError("Unsupported Parquet compression")

    _positive(settings.backtesting.lookback_days, "backtesting.lookback_days")
    _positive(settings.backtesting.horizon_days, "backtesting.horizon_days")
    if settings.backtesting.annual_holding_cost_rate < 0 or settings.backtesting.ordering_cost < 0:
        raise ValueError("Backtesting economic costs must be non-negative")
    if not 0 <= settings.backtesting.service_tolerance <= 1:
        raise ValueError("backtesting.service_tolerance must be in [0, 1]")
    if not settings.backtesting.fold_start_dates:
        raise ValueError("At least one backtesting fold is required")
    fold_dates = [str(value) for value in settings.backtesting.fold_start_dates]
    if len(set(fold_dates)) != len(fold_dates):
        raise ValueError("Backtesting fold_start_dates must be unique")
    try:
        for value in fold_dates:
            parsed = date.fromisoformat(value)
            if value != parsed.isoformat():
                raise ValueError
    except ValueError as exc:
        raise ValueError("Backtesting fold_start_dates must use YYYY-MM-DD") from exc
    if not settings.backtesting.policies:
        raise ValueError("At least one backtesting policy is required")
    if len({policy.policy_id for policy in settings.backtesting.policies}) != len(
        settings.backtesting.policies
    ):
        raise ValueError("Backtesting policy_id values must be unique")
    for policy in settings.backtesting.policies:
        _positive(policy.safety_stock_factor, f"policy {policy.policy_id} safety_stock_factor")
        _positive(policy.cycle_stock_days, f"policy {policy.policy_id} cycle_stock_days")

    _positive(settings.monte_carlo.simulations, "monte_carlo.simulations")
    _positive(settings.monte_carlo.warmup_days, "monte_carlo.warmup_days")
    _positive(settings.monte_carlo.horizon_days, "monte_carlo.horizon_days")
    _positive(settings.monte_carlo.demand_block_days, "monte_carlo.demand_block_days")
    _positive(settings.monte_carlo.target_entities, "monte_carlo.target_entities")
    if not 0 < settings.monte_carlo.target_confidence <= 1:
        raise ValueError("monte_carlo.target_confidence must be in (0, 1]")
    if settings.monte_carlo.max_inventory_increase_rate < 0:
        raise ValueError("monte_carlo.max_inventory_increase_rate must be non-negative")
    if settings.monte_carlo.annual_holding_cost_rate < 0 or settings.monte_carlo.ordering_cost < 0:
        raise ValueError("Monte Carlo economic costs must be non-negative")
    if not settings.monte_carlo.variants:
        raise ValueError("At least one Monte Carlo variant is required")
    if len({variant.scenario_id for variant in settings.monte_carlo.variants}) != len(
        settings.monte_carlo.variants
    ):
        raise ValueError("Monte Carlo scenario_id values must be unique")
    for variant in settings.monte_carlo.variants:
        _positive(
            variant.safety_factor_multiplier,
            f"scenario {variant.scenario_id} safety_factor_multiplier",
        )
    _positive(
        settings.action_tracking.measurement_window_days,
        "action_tracking.measurement_window_days",
    )

    try:
        parsed_as_of_date = date.fromisoformat(settings.source_governance.as_of_date)
        if settings.source_governance.as_of_date != parsed_as_of_date.isoformat():
            raise ValueError
    except ValueError as exc:
        raise ValueError("source_governance.as_of_date must use YYYY-MM-DD") from exc
    if not settings.source_governance.registry_path.strip():
        raise ValueError("source_governance.registry_path must not be empty")
    required_source_policies = canonical_tables
    missing_source_policies = required_source_policies - set(
        settings.source_governance.table_policies
    )
    if missing_source_policies:
        raise ValueError(
            "source_governance.table_policies missing tables: "
            + ", ".join(sorted(missing_source_policies))
        )
    unknown_source_policies = set(settings.source_governance.table_policies) - canonical_tables
    if unknown_source_policies:
        raise ValueError(
            "source_governance.table_policies contains unknown tables: "
            + ", ".join(sorted(unknown_source_policies))
        )
    for table_name, source_policy in settings.source_governance.table_policies.items():
        if not source_policy.source_system.strip() or not source_policy.owner.strip():
            raise ValueError(f"source policy {table_name} requires source_system and owner")
        if source_policy.max_lag_hours is not None:
            _positive(
                source_policy.max_lag_hours,
                f"source policy {table_name} max_lag_hours",
            )
            if not source_policy.watermark_column:
                raise ValueError(
                    f"source policy {table_name} requires watermark_column with max_lag_hours"
                )
        if source_policy.stale_severity not in {"WARN", "FAIL"}:
            raise ValueError(f"source policy {table_name} stale_severity must be WARN or FAIL")

    _positive(settings.causal_evaluation.permutations, "causal_evaluation.permutations")
    if settings.causal_evaluation.min_units_per_group < 2:
        raise ValueError("causal_evaluation.min_units_per_group must be at least 2")
    _positive(settings.causal_evaluation.pre_days, "causal_evaluation.pre_days")
    _positive(settings.causal_evaluation.post_days, "causal_evaluation.post_days")
    if not 0 < settings.causal_evaluation.alpha < 1:
        raise ValueError("causal_evaluation.alpha must be in (0, 1)")
    if not 0 < settings.causal_evaluation.parallel_trend_alpha < 1:
        raise ValueError("causal_evaluation.parallel_trend_alpha must be in (0, 1)")

    _positive(settings.network_optimization.horizon_days, "network_optimization.horizon_days")
    _positive(
        settings.network_optimization.demand_lookback_days,
        "network_optimization.demand_lookback_days",
    )
    if (
        settings.network_optimization.annual_holding_cost_rate < 0
        or settings.network_optimization.ordering_cost < 0
    ):
        raise ValueError("network_optimization economic costs must be non-negative")
    _positive(
        settings.network_optimization.shortage_penalty_multiplier,
        "network_optimization.shortage_penalty_multiplier",
    )
    _positive(
        settings.network_optimization.time_limit_seconds,
        "network_optimization.time_limit_seconds",
    )
    if not 0 <= settings.network_optimization.mip_relative_gap < 1:
        raise ValueError("network_optimization.mip_relative_gap must be in [0, 1)")

    if settings.orchestration.max_retries < 0:
        raise ValueError("orchestration.max_retries must be non-negative")
    _positive(settings.orchestration.retry_base_seconds, "orchestration.retry_base_seconds")
    object_store = settings.orchestration.object_store
    if object_store.backend not in {"local", "s3"}:
        raise ValueError("orchestration.object_store.backend must be local or s3")
    if not object_store.local_root.strip():
        raise ValueError("orchestration.object_store.local_root must not be empty")
    if object_store.backend == "s3" and not object_store.s3_bucket:
        raise ValueError("orchestration.object_store.s3_bucket is required for s3")
    if not object_store.s3_prefix.strip() or not object_store.s3_region.strip():
        raise ValueError("orchestration S3 prefix and region must not be empty")
    if object_store.s3_server_side_encryption not in {"AES256", "aws:kms"}:
        raise ValueError("Unsupported S3 server-side encryption")


def load_settings(path: Path | None = None) -> PipelineSettings:
    """Load the versioned pipeline config, with a path override via environment."""
    configured_path = path or Path(os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH))
    if not configured_path.is_absolute():
        configured_path = PROJECT_ROOT / configured_path
    payload = json.loads(configured_path.read_text(encoding="utf-8"))

    adapter_data = payload["adapter"]
    backtest_data = payload["backtesting"]
    monte_carlo_data = payload["monte_carlo"]
    source_governance_data = payload["source_governance"]
    causal_data = payload["causal_evaluation"]
    network_data = payload["network_optimization"]
    orchestration_data = payload["orchestration"]
    object_store_data = orchestration_data["object_store"]
    settings = PipelineSettings(
        version=int(payload["version"]),
        adapter=AdapterSettings(
            type=os.environ.get("INGESTION_ADAPTER", adapter_data["type"]),
            source_path=os.environ.get("SOURCE_DATA_PATH", adapter_data.get("source_path")),
            erp_path=os.environ.get("ERP_EXPORT_PATH", adapter_data.get("erp_path")),
            wms_path=os.environ.get("WMS_EXPORT_PATH", adapter_data.get("wms_path")),
            file_format=str(adapter_data.get("file_format", "csv")),
            column_mapping=dict(adapter_data.get("column_mapping", {})),
        ),
        storage=StorageSettings(**payload["storage"]),
        backtesting=BacktestSettings(
            lookback_days=int(backtest_data["lookback_days"]),
            horizon_days=int(backtest_data["horizon_days"]),
            fold_start_dates=tuple(backtest_data["fold_start_dates"]),
            annual_holding_cost_rate=float(backtest_data["annual_holding_cost_rate"]),
            ordering_cost=float(backtest_data["ordering_cost"]),
            service_tolerance=float(backtest_data["service_tolerance"]),
            policies=tuple(PolicySettings(**policy) for policy in backtest_data["policies"]),
        ),
        monte_carlo=MonteCarloSettings(
            seed=int(monte_carlo_data["seed"]),
            simulations=int(monte_carlo_data["simulations"]),
            warmup_days=int(monte_carlo_data["warmup_days"]),
            horizon_days=int(monte_carlo_data["horizon_days"]),
            demand_block_days=int(monte_carlo_data["demand_block_days"]),
            target_entities=int(monte_carlo_data["target_entities"]),
            target_confidence=float(monte_carlo_data["target_confidence"]),
            max_inventory_increase_rate=float(monte_carlo_data["max_inventory_increase_rate"]),
            annual_holding_cost_rate=float(monte_carlo_data["annual_holding_cost_rate"]),
            ordering_cost=float(monte_carlo_data["ordering_cost"]),
            variants=tuple(
                MonteCarloVariant(**variant) for variant in monte_carlo_data["variants"]
            ),
        ),
        action_tracking=ActionTrackingSettings(**payload["action_tracking"]),
        source_governance=SourceGovernanceSettings(
            enabled=bool(source_governance_data["enabled"]),
            as_of_date=os.environ.get(
                "PIPELINE_AS_OF_DATE", str(source_governance_data["as_of_date"])
            ),
            registry_path=str(source_governance_data["registry_path"]),
            table_policies={
                table_name: SourceTablePolicy(
                    source_system=str(policy["source_system"]),
                    owner=str(policy["owner"]),
                    watermark_column=(
                        str(policy["watermark_column"])
                        if policy.get("watermark_column") is not None
                        else None
                    ),
                    max_lag_hours=(
                        int(policy["max_lag_hours"])
                        if policy.get("max_lag_hours") is not None
                        else None
                    ),
                    stale_severity=str(policy["stale_severity"]),
                )
                for table_name, policy in source_governance_data["table_policies"].items()
            },
        ),
        causal_evaluation=CausalEvaluationSettings(
            enabled=bool(causal_data["enabled"]),
            seed=int(causal_data["seed"]),
            permutations=int(causal_data["permutations"]),
            min_units_per_group=int(causal_data["min_units_per_group"]),
            pre_days=int(causal_data["pre_days"]),
            post_days=int(causal_data["post_days"]),
            alpha=float(causal_data["alpha"]),
            parallel_trend_alpha=float(causal_data["parallel_trend_alpha"]),
        ),
        network_optimization=NetworkOptimizationSettings(
            enabled=bool(network_data["enabled"]),
            horizon_days=int(network_data["horizon_days"]),
            demand_lookback_days=int(network_data["demand_lookback_days"]),
            annual_holding_cost_rate=float(network_data["annual_holding_cost_rate"]),
            shortage_penalty_multiplier=float(network_data["shortage_penalty_multiplier"]),
            ordering_cost=float(network_data["ordering_cost"]),
            time_limit_seconds=int(network_data["time_limit_seconds"]),
            mip_relative_gap=float(network_data["mip_relative_gap"]),
        ),
        orchestration=OrchestrationSettings(
            max_retries=int(orchestration_data["max_retries"]),
            retry_base_seconds=float(orchestration_data["retry_base_seconds"]),
            object_store=ObjectStoreSettings(
                backend=os.environ.get("OBJECT_STORE_BACKEND", object_store_data["backend"]),
                local_root=os.environ.get("OBJECT_STORE_ROOT", object_store_data["local_root"]),
                s3_bucket=os.environ.get("S3_BUCKET", object_store_data.get("s3_bucket")),
                s3_prefix=os.environ.get("S3_PREFIX", object_store_data["s3_prefix"]),
                s3_endpoint_url=os.environ.get(
                    "S3_ENDPOINT_URL", object_store_data.get("s3_endpoint_url")
                ),
                s3_region=os.environ.get("AWS_REGION", object_store_data["s3_region"]),
                s3_server_side_encryption=str(object_store_data["s3_server_side_encryption"]),
            ),
        ),
    )
    _validate(settings)
    return settings
