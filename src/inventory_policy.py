"""Deterministic inventory-policy simulation shared by backtests and scenarios."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class PolicyParameters:
    policy_id: str
    safety_stock_factor: float
    cycle_stock_days: int


@dataclass(frozen=True)
class PolicySimulation:
    policy_id: str
    reorder_point: int
    order_up_to: int
    fill_rate: float
    units_demanded: int
    units_fulfilled: int
    units_lost: int
    average_inventory_units: float
    average_inventory_value: float
    evaluation_starting_on_hand: int
    ending_on_hand: int
    order_count: int
    ordered_units: int
    received_units: int
    lost_margin_proxy: float
    holding_cost_proxy: float
    ordering_cost_proxy: float
    economic_cost_proxy: float
    balance_error_units: int


def policy_levels(
    mean_daily_demand: float,
    demand_std: float,
    lead_time_days: int,
    service_z: float,
    policy: PolicyParameters,
) -> tuple[int, int]:
    if mean_daily_demand < 0 or demand_std < 0:
        raise ValueError("Demand parameters must be non-negative")
    if lead_time_days < 1:
        raise ValueError("lead_time_days must be positive")
    if policy.safety_stock_factor <= 0 or policy.cycle_stock_days < 1:
        raise ValueError("Policy factors must be positive")

    safety_stock = policy.safety_stock_factor * service_z * demand_std * math.sqrt(lead_time_days)
    reorder_point = math.ceil(mean_daily_demand * lead_time_days + safety_stock)
    order_up_to = math.ceil(reorder_point + mean_daily_demand * policy.cycle_stock_days)
    return max(0, reorder_point), max(reorder_point, order_up_to)


def simulate_policy(
    demand: np.ndarray,
    starting_on_hand: int,
    mean_daily_demand: float,
    demand_std: float,
    lead_time_days: int,
    service_z: float,
    minimum_order_qty: int,
    unit_cost: float,
    unit_margin: float,
    annual_holding_cost_rate: float,
    ordering_cost: float,
    policy: PolicyParameters,
    warmup_days: int = 0,
) -> PolicySimulation:
    """Simulate a policy, excluding optional initialization days from reported KPIs."""
    demand = np.asarray(demand, dtype=int)
    if demand.ndim != 1 or len(demand) == 0:
        raise ValueError("demand must be a non-empty one-dimensional array")
    if (demand < 0).any() or starting_on_hand < 0 or minimum_order_qty < 1:
        raise ValueError("Demand, starting inventory, and MOQ inputs are invalid")
    if warmup_days < 0 or warmup_days >= len(demand):
        raise ValueError("warmup_days must be in [0, len(demand))")
    if min(unit_cost, unit_margin, annual_holding_cost_rate, ordering_cost) < 0:
        raise ValueError("Economic inputs must be non-negative")

    reorder_point, order_up_to = policy_levels(
        mean_daily_demand,
        demand_std,
        lead_time_days,
        service_z,
        policy,
    )
    receipts = np.zeros(len(demand) + lead_time_days + 1, dtype=int)
    on_hand = int(starting_on_hand)
    on_order = 0
    fulfilled_total = 0
    lost_total = 0
    received_total = 0
    ordered_total = 0
    order_count = 0
    inventory_sum = 0
    evaluation_starting_on_hand = starting_on_hand

    for day, units_demanded in enumerate(demand):
        if day == warmup_days:
            evaluation_starting_on_hand = on_hand
        received_today = int(receipts[day])
        on_hand += received_today
        on_order -= received_today
        if day >= warmup_days:
            received_total += received_today

        fulfilled = min(on_hand, int(units_demanded))
        lost = int(units_demanded) - fulfilled
        on_hand -= fulfilled
        if day >= warmup_days:
            fulfilled_total += fulfilled
            lost_total += lost

        inventory_position = on_hand + on_order
        if inventory_position <= reorder_point:
            order_quantity = max(order_up_to - inventory_position, minimum_order_qty)
            arrival_day = day + lead_time_days
            receipts[arrival_day] += order_quantity
            on_order += order_quantity
            if day >= warmup_days:
                ordered_total += order_quantity
                order_count += 1

        if day >= warmup_days:
            inventory_sum += on_hand

    evaluation_days = len(demand) - warmup_days
    demanded_total = int(demand[warmup_days:].sum())
    fill_rate = fulfilled_total / demanded_total if demanded_total else 1.0
    average_inventory_units = inventory_sum / evaluation_days
    average_inventory_value = average_inventory_units * unit_cost
    lost_margin_proxy = lost_total * unit_margin
    holding_cost_proxy = (
        average_inventory_value * annual_holding_cost_rate * evaluation_days / 365.0
    )
    ordering_cost_proxy = order_count * ordering_cost
    balance_error = evaluation_starting_on_hand + received_total - fulfilled_total - on_hand

    return PolicySimulation(
        policy_id=policy.policy_id,
        reorder_point=reorder_point,
        order_up_to=order_up_to,
        fill_rate=fill_rate,
        units_demanded=demanded_total,
        units_fulfilled=fulfilled_total,
        units_lost=lost_total,
        average_inventory_units=average_inventory_units,
        average_inventory_value=average_inventory_value,
        evaluation_starting_on_hand=evaluation_starting_on_hand,
        ending_on_hand=on_hand,
        order_count=order_count,
        ordered_units=ordered_total,
        received_units=received_total,
        lost_margin_proxy=lost_margin_proxy,
        holding_cost_proxy=holding_cost_proxy,
        ordering_cost_proxy=ordering_cost_proxy,
        economic_cost_proxy=lost_margin_proxy + holding_cost_proxy + ordering_cost_proxy,
        balance_error_units=balance_error,
    )
