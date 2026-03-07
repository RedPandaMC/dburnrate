"""Pydantic models for dburnrate."""

from typing import Literal

from pydantic import BaseModel, ConfigDict


class OperationInfo(BaseModel):
    """Information about a database operation."""

    name: str
    kind: str
    weight: float


class QueryProfile(BaseModel):
    """Profile of a SQL query with complexity analysis."""

    sql: str
    dialect: str = "databricks"
    operations: list[OperationInfo] = []
    tables: list[str] = []
    complexity_score: float = 0.0


class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""

    model_config = ConfigDict(frozen=True)
    instance_type: str = "Standard_DS3_v2"
    num_workers: int = 2
    dbu_per_hour: float = 0.75
    photon_enabled: bool = False


class PricingInfo(BaseModel):
    """Pricing information for a SKU."""

    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"


class CostEstimate(BaseModel):
    """Cost estimate for a query or workload."""

    estimated_dbu: float
    estimated_cost_usd: float | None = None
    estimated_cost_eur: float | None = None
    confidence: Literal["low", "medium", "high"] = "low"
    breakdown: dict[str, float] = {}
    warnings: list[str] = []


class ClusterRecommendation(BaseModel):
    """Recommendation for cluster optimization."""

    current_config: ClusterConfig
    recommended_config: ClusterConfig
    bottleneck: list[str] = []
    estimated_savings_pct: float
    confidence: Literal["low", "medium", "high"]
    reason: str
