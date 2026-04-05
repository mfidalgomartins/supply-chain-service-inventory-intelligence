from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

try:
    from src.config import DATA_RAW, END_DATE, RANDOM_SEED, START_DATE
except ModuleNotFoundError:
    from config import DATA_RAW, END_DATE, RANDOM_SEED, START_DATE


@dataclass(frozen=True)
class SimulationConfig:
    """Configuration for synthetic supply chain data generation."""

    seed: int = RANDOM_SEED
    start_date: str = START_DATE
    end_date: str = END_DATE
    n_products: int = 120
    n_suppliers: int = 12


def _bounded(value: float, low: float, high: float) -> float:
    return float(np.clip(value, low, high))


def build_suppliers(cfg: SimulationConfig, rng: np.random.Generator) -> pd.DataFrame:
    """Create supplier master with intentional risk profile diversity."""
    regions = ["Iberia", "Central Europe", "UK", "Nordics", "East Europe"]
    records: list[dict] = []

    for idx in range(1, cfg.n_suppliers + 1):
        supplier_id = f"SUP-{idx:03d}"

        if idx <= 3:
            reliability = _bounded(rng.normal(0.74, 0.05), 0.60, 0.86)
            avg_lt = int(np.clip(round(rng.normal(22, 3.5)), 15, 30))
            lt_var = _bounded(rng.normal(0.42, 0.08), 0.25, 0.65)
            moq = int(np.clip(round(rng.normal(750, 130)), 500, 1000))
        elif idx <= 8:
            reliability = _bounded(rng.normal(0.86, 0.04), 0.77, 0.93)
            avg_lt = int(np.clip(round(rng.normal(14, 2.5)), 8, 22))
            lt_var = _bounded(rng.normal(0.24, 0.05), 0.12, 0.38)
            moq = int(np.clip(round(rng.normal(420, 90)), 240, 640))
        else:
            reliability = _bounded(rng.normal(0.95, 0.02), 0.90, 0.99)
            avg_lt = int(np.clip(round(rng.normal(8, 1.8)), 4, 14))
            lt_var = _bounded(rng.normal(0.12, 0.03), 0.06, 0.20)
            moq = int(np.clip(round(rng.normal(180, 50)), 80, 320))

        records.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": f"Supplier {idx}",
                "supplier_region": rng.choice(regions, p=[0.33, 0.27, 0.12, 0.13, 0.15]),
                "reliability_score": round(reliability, 3),
                "average_lead_time_days": int(avg_lt),
                "lead_time_variability": round(lt_var, 3),
                "minimum_order_qty": int(moq),
            }
        )

    return pd.DataFrame(records)


def build_warehouses() -> pd.DataFrame:
    """Create warehouse network with regional demand and execution differences."""
    return pd.DataFrame(
        [
            {
                "warehouse_id": "WH-LIS",
                "warehouse_name": "Lisbon Distribution Center",
                "region": "Portugal South",
                "storage_capacity_units": 1_850_000,
            },
            {
                "warehouse_id": "WH-PORTO",
                "warehouse_name": "Porto Distribution Center",
                "region": "Portugal North",
                "storage_capacity_units": 1_420_000,
            },
            {
                "warehouse_id": "WH-MAD",
                "warehouse_name": "Madrid Regional Hub",
                "region": "Spain Central",
                "storage_capacity_units": 1_650_000,
            },
            {
                "warehouse_id": "WH-LYON",
                "warehouse_name": "Lyon EU Gateway",
                "region": "France South-East",
                "storage_capacity_units": 1_120_000,
            },
        ]
    )


def build_products_and_classification(
    cfg: SimulationConfig, suppliers: pd.DataFrame, rng: np.random.Generator
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create product master + ABC/criticality classification + hidden simulation attributes."""
    categories = [
        "Beverages",
        "Snacks",
        "Personal Care",
        "Household",
        "Frozen",
        "Dairy",
        "Pet Care",
        "Health",
    ]

    category_cost = {
        "Beverages": (2.2, 18.0),
        "Snacks": (1.5, 12.0),
        "Personal Care": (3.5, 28.0),
        "Household": (4.0, 35.0),
        "Frozen": (2.8, 20.0),
        "Dairy": (1.8, 10.0),
        "Pet Care": (4.5, 42.0),
        "Health": (8.0, 70.0),
    }

    category_shelf_life = {
        "Beverages": (180, 540),
        "Snacks": (120, 360),
        "Personal Care": (365, 1095),
        "Household": (365, 1095),
        "Frozen": (300, 720),
        "Dairy": (12, 45),
        "Pet Care": (210, 720),
        "Health": (365, 1460),
    }

    abc_mix = ["A"] * int(cfg.n_products * 0.20) + ["B"] * int(cfg.n_products * 0.30)
    abc_mix += ["C"] * (cfg.n_products - len(abc_mix))

    supplier_weights = suppliers["reliability_score"].to_numpy()
    supplier_weights = supplier_weights / supplier_weights.sum()

    chronic_profiles = np.array(["normal"] * cfg.n_products, dtype=object)
    overstock_count = max(8, int(cfg.n_products * 0.12))
    stockout_count = max(8, int(cfg.n_products * 0.12))
    overstock_idx = rng.choice(np.arange(cfg.n_products), size=overstock_count, replace=False)
    remaining = np.setdiff1d(np.arange(cfg.n_products), overstock_idx)
    stockout_idx = rng.choice(remaining, size=stockout_count, replace=False)
    chronic_profiles[overstock_idx] = "chronic_overstock"
    chronic_profiles[stockout_idx] = "chronic_stockout"

    product_rows: list[dict] = []
    class_rows: list[dict] = []
    sim_rows: list[dict] = []

    for idx in range(cfg.n_products):
        product_id = f"SKU-{idx + 1:04d}"
        category = categories[idx % len(categories)]
        abc = abc_mix[idx]

        if chronic_profiles[idx] == "chronic_stockout":
            supplier = suppliers.iloc[int(rng.integers(0, min(3, len(suppliers))))]
        else:
            supplier = suppliers.iloc[int(rng.choice(np.arange(len(suppliers)), p=supplier_weights))]

        c_min, c_max = category_cost[category]
        unit_cost = round(float(rng.uniform(c_min, c_max)), 2)
        markup = float(rng.uniform(1.25, 2.4))
        unit_price = round(unit_cost * markup, 2)

        sl_min, sl_max = category_shelf_life[category]
        shelf_life = int(rng.integers(sl_min, sl_max + 1))

        lead_time_days = int(
            max(1, round(rng.normal(supplier["average_lead_time_days"], supplier["average_lead_time_days"] * 0.08)))
        )

        if abc == "A":
            target_service = _bounded(rng.normal(0.98, 0.01), 0.95, 0.995)
            base_demand = _bounded(rng.normal(85, 18), 35, 160)
            demand_cv = _bounded(rng.normal(0.22, 0.05), 0.12, 0.38)
            target_cover_days = int(np.clip(round(rng.normal(16, 3)), 10, 24))
        elif abc == "B":
            target_service = _bounded(rng.normal(0.95, 0.015), 0.90, 0.985)
            base_demand = _bounded(rng.normal(42, 12), 12, 85)
            demand_cv = _bounded(rng.normal(0.30, 0.06), 0.18, 0.48)
            target_cover_days = int(np.clip(round(rng.normal(25, 4)), 16, 36))
        else:
            target_service = _bounded(rng.normal(0.90, 0.02), 0.84, 0.95)
            base_demand = _bounded(rng.normal(13, 6), 2, 35)
            demand_cv = _bounded(rng.normal(0.45, 0.08), 0.25, 0.70)
            target_cover_days = int(np.clip(round(rng.normal(38, 7)), 24, 56))

        if chronic_profiles[idx] == "chronic_overstock":
            planning_bias = _bounded(rng.normal(1.45, 0.10), 1.25, 1.70)
        elif chronic_profiles[idx] == "chronic_stockout":
            planning_bias = _bounded(rng.normal(0.78, 0.08), 0.55, 0.92)
        else:
            planning_bias = _bounded(rng.normal(1.0, 0.12), 0.80, 1.25)

        if abc == "A" or category in {"Health", "Dairy", "Frozen"}:
            criticality = "High"
        elif abc == "B":
            criticality = "Medium"
        else:
            criticality = "Low"

        product_rows.append(
            {
                "product_id": product_id,
                "product_name": f"{category} Product {idx + 1}",
                "category": category,
                "unit_cost": unit_cost,
                "unit_price": unit_price,
                "shelf_life_days": shelf_life,
                "supplier_id": supplier["supplier_id"],
                "lead_time_days": lead_time_days,
                "target_service_level": round(target_service, 3),
            }
        )

        class_rows.append({"product_id": product_id, "abc_class": abc, "criticality_level": criticality})

        sim_rows.append(
            {
                "product_id": product_id,
                "base_daily_demand": round(base_demand, 3),
                "demand_cv": round(demand_cv, 3),
                "target_cover_days": target_cover_days,
                "planning_bias": round(planning_bias, 3),
                "chronic_profile": chronic_profiles[idx],
            }
        )

    return pd.DataFrame(product_rows), pd.DataFrame(class_rows), pd.DataFrame(sim_rows)


def seasonality_index(date: pd.Timestamp) -> float:
    month_factor = {
        1: 0.93,
        2: 0.95,
        3: 1.00,
        4: 1.04,
        5: 1.02,
        6: 1.08,
        7: 1.12,
        8: 1.10,
        9: 1.03,
        10: 1.05,
        11: 1.11,
        12: 1.24,
    }[date.month]
    weekday_factor = 0.92 if date.weekday() in (5, 6) else 1.0
    return round(month_factor * weekday_factor, 3)


def simulate_operations(
    cfg: SimulationConfig,
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    sim_attrs: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Simulate daily demand, inventory states, and purchase order execution."""
    dates = pd.date_range(cfg.start_date, cfg.end_date, freq="D")

    warehouse_profile = {
        "WH-LIS": {"demand_factor": 1.15, "planning_factor": 1.12, "volatility_factor": 0.95},
        "WH-PORTO": {"demand_factor": 0.95, "planning_factor": 1.03, "volatility_factor": 1.00},
        "WH-MAD": {"demand_factor": 1.05, "planning_factor": 0.93, "volatility_factor": 1.08},
        "WH-LYON": {"demand_factor": 0.84, "planning_factor": 0.90, "volatility_factor": 1.12},
    }

    product_lookup = products.set_index("product_id").to_dict(orient="index")
    supplier_lookup = suppliers.set_index("supplier_id").to_dict(orient="index")
    sim_lookup = sim_attrs.set_index("product_id").to_dict(orient="index")

    demand_rows: list[dict] = []
    inventory_rows: list[dict] = []
    po_rows: list[dict] = []

    po_counter = 0

    for product in products["product_id"]:
        p = product_lookup[product]
        s = supplier_lookup[p["supplier_id"]]
        sa = sim_lookup[product]

        for _, wh_row in warehouses.iterrows():
            wh_id = wh_row["warehouse_id"]
            wh_prof = warehouse_profile[wh_id]

            local_demand_mean = sa["base_daily_demand"] * wh_prof["demand_factor"] * float(rng.uniform(0.92, 1.12))
            local_demand_cv = _bounded(sa["demand_cv"] * wh_prof["volatility_factor"], 0.08, 0.90)
            policy_bias = sa["planning_bias"] * wh_prof["planning_factor"]

            reorder_point = int(local_demand_mean * (p["lead_time_days"] + 2) * policy_bias)
            order_up_to = int(local_demand_mean * (p["lead_time_days"] + sa["target_cover_days"]) * policy_bias)

            on_hand = max(40, int(order_up_to * rng.uniform(0.65, 1.05)))
            open_orders: list[dict] = []

            for current_date in dates:
                arrivals_today = [o for o in open_orders if o["actual_arrival_date"] == current_date]
                if arrivals_today:
                    on_hand += int(sum(o["received_units"] for o in arrivals_today))
                open_orders = [o for o in open_orders if o["actual_arrival_date"] > current_date]

                seasonality = seasonality_index(current_date)
                promo_probability = 0.04 + (0.04 if current_date.month in (11, 12) else 0.0)
                promo_probability += 0.03 if p["category"] in {"Snacks", "Beverages"} else 0.0
                promo_flag = int(rng.random() < min(0.22, promo_probability))
                promo_lift = _bounded(rng.normal(1.22, 0.09), 1.05, 1.45) if promo_flag == 1 else 1.0

                demand_mean = local_demand_mean * seasonality * promo_lift
                demand_std = max(1.0, demand_mean * local_demand_cv)
                units_demanded = int(max(0, round(rng.normal(demand_mean, demand_std))))

                if sa["chronic_profile"] == "chronic_stockout":
                    units_demanded = int(round(units_demanded * _bounded(rng.normal(1.08, 0.04), 1.0, 1.18)))

                units_fulfilled = int(min(on_hand, units_demanded))
                units_lost_sales = int(units_demanded - units_fulfilled)
                stockout_flag = int(units_lost_sales > 0)

                on_hand -= units_fulfilled
                on_order_units = int(sum(o["received_units"] for o in open_orders))
                inventory_position = on_hand + on_order_units

                if inventory_position <= reorder_point:
                    moq_multiplier = 1.4 if sa["chronic_profile"] == "chronic_overstock" else 1.0
                    effective_moq = int(round(s["minimum_order_qty"] * moq_multiplier))
                    ordered_units = int(max(order_up_to - inventory_position, effective_moq))

                    planned_lt = int(p["lead_time_days"])
                    lead_time_noise = max(1.0, s["average_lead_time_days"] * s["lead_time_variability"])
                    realized_lt = int(max(1, round(rng.normal(planned_lt, lead_time_noise))))

                    delay_days = 0
                    if rng.random() > s["reliability_score"]:
                        delay_days = int(rng.integers(2, 10))
                    expected_arrival = current_date + timedelta(days=planned_lt)
                    actual_arrival = current_date + timedelta(days=realized_lt + delay_days)

                    receipt_fill_rate = _bounded(rng.normal(0.985, 0.015), 0.9, 1.0)
                    if s["reliability_score"] < 0.80:
                        receipt_fill_rate = _bounded(rng.normal(0.93, 0.06), 0.78, 1.0)
                    received_units = int(max(1, round(ordered_units * receipt_fill_rate)))

                    po_counter += 1
                    po_id = f"PO-{po_counter:08d}"
                    late_flag = int(actual_arrival > expected_arrival)

                    po_entry = {
                        "po_id": po_id,
                        "supplier_id": p["supplier_id"],
                        "product_id": product,
                        "warehouse_id": wh_id,
                        "order_date": current_date,
                        "expected_arrival_date": expected_arrival,
                        "actual_arrival_date": actual_arrival,
                        "ordered_units": ordered_units,
                        "received_units": received_units,
                        "late_delivery_flag": late_flag,
                    }
                    po_rows.append(po_entry)
                    open_orders.append(po_entry)

                    on_order_units = int(sum(o["received_units"] for o in open_orders))

                reserved_units = int(min(on_hand, round(local_demand_mean * rng.uniform(0.04, 0.22))))
                available_units = int(max(0, on_hand - reserved_units))
                inventory_value = round(on_hand * p["unit_cost"], 2)
                days_of_supply = round(available_units / max(1.0, local_demand_mean), 2)

                demand_rows.append(
                    {
                        "date": current_date,
                        "warehouse_id": wh_id,
                        "product_id": product,
                        "region": wh_row["region"],
                        "units_demanded": units_demanded,
                        "units_fulfilled": units_fulfilled,
                        "units_lost_sales": units_lost_sales,
                        "stockout_flag": stockout_flag,
                        "promo_flag": promo_flag,
                        "seasonality_index": seasonality,
                    }
                )

                inventory_rows.append(
                    {
                        "snapshot_date": current_date,
                        "warehouse_id": wh_id,
                        "product_id": product,
                        "on_hand_units": int(on_hand),
                        "on_order_units": int(on_order_units),
                        "reserved_units": int(reserved_units),
                        "available_units": int(available_units),
                        "inventory_value": inventory_value,
                        "days_of_supply": days_of_supply,
                    }
                )

    demand_history = pd.DataFrame(demand_rows)
    inventory_snapshots = pd.DataFrame(inventory_rows)
    purchase_orders = pd.DataFrame(po_rows)

    return demand_history, inventory_snapshots, purchase_orders


def write_raw_tables(
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    inventory_snapshots: pd.DataFrame,
    demand_history: pd.DataFrame,
    purchase_orders: pd.DataFrame,
    product_classification: pd.DataFrame,
) -> None:
    """Persist required raw tables to /data/raw/."""
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    products.to_csv(DATA_RAW / "products.csv", index=False)
    suppliers.to_csv(DATA_RAW / "suppliers.csv", index=False)
    warehouses.to_csv(DATA_RAW / "warehouses.csv", index=False)
    inventory_snapshots.to_csv(DATA_RAW / "inventory_snapshots.csv", index=False)
    demand_history.to_csv(DATA_RAW / "demand_history.csv", index=False)
    purchase_orders.to_csv(DATA_RAW / "purchase_orders.csv", index=False)
    product_classification.to_csv(DATA_RAW / "product_classification.csv", index=False)


def print_summary(
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    inventory_snapshots: pd.DataFrame,
    demand_history: pd.DataFrame,
    purchase_orders: pd.DataFrame,
    product_classification: pd.DataFrame,
) -> None:
    """Print concise dataset quality and scale summary."""
    stockout_rate = demand_history["stockout_flag"].mean()
    fill_rate = demand_history["units_fulfilled"].sum() / max(1, demand_history["units_demanded"].sum())

    demand_with_inventory = demand_history.merge(
        inventory_snapshots,
        left_on=["date", "warehouse_id", "product_id"],
        right_on=["snapshot_date", "warehouse_id", "product_id"],
        how="inner",
    )
    excess_flag = (demand_with_inventory["days_of_supply"] > 35).astype(int)

    late_rate = purchase_orders["late_delivery_flag"].mean() if not purchase_orders.empty else 0.0

    sku_dos = (
        inventory_snapshots.groupby("product_id", as_index=False)["days_of_supply"]
        .mean()
        .rename(columns={"days_of_supply": "avg_days_of_supply"})
    )
    sku_stockout = (
        demand_history.groupby("product_id", as_index=False)["stockout_flag"]
        .mean()
        .rename(columns={"stockout_flag": "stockout_day_rate"})
    )
    sku_behavior = sku_dos.merge(sku_stockout, on="product_id", how="inner")
    chronic_overstock_count = int((sku_behavior["avg_days_of_supply"] >= 35).sum())
    chronic_stockout_count = int((sku_behavior["stockout_day_rate"] >= 0.12).sum())

    print("Synthetic data generation complete.")
    print(
        f"Rows | products: {len(products):,}, suppliers: {len(suppliers):,}, warehouses: {len(warehouses):,}, "
        f"inventory_snapshots: {len(inventory_snapshots):,}, demand_history: {len(demand_history):,}, "
        f"purchase_orders: {len(purchase_orders):,}, product_classification: {len(product_classification):,}"
    )
    print(
        f"Health | fill_rate: {fill_rate:.3f}, stockout_day_rate: {stockout_rate:.3f}, "
        f"late_delivery_rate: {late_rate:.3f}, excess_inventory_day_rate: {excess_flag.mean():.3f}"
    )
    print(f"Coverage | {demand_history['date'].min()} to {demand_history['date'].max()}")
    print(
        "Signal | "
        f"chronic_overstock_skus: {chronic_overstock_count}, "
        f"chronic_stockout_skus: {chronic_stockout_count}"
    )


def generate_all_tables() -> None:
    cfg = SimulationConfig()
    rng = np.random.default_rng(cfg.seed)

    suppliers = build_suppliers(cfg, rng)
    warehouses = build_warehouses()
    products, product_classification, sim_attrs = build_products_and_classification(cfg, suppliers, rng)

    demand_history, inventory_snapshots, purchase_orders = simulate_operations(
        cfg=cfg,
        products=products,
        suppliers=suppliers,
        warehouses=warehouses,
        sim_attrs=sim_attrs,
        rng=rng,
    )

    write_raw_tables(
        products=products,
        suppliers=suppliers,
        warehouses=warehouses,
        inventory_snapshots=inventory_snapshots,
        demand_history=demand_history,
        purchase_orders=purchase_orders,
        product_classification=product_classification,
    )

    print_summary(
        products=products,
        suppliers=suppliers,
        warehouses=warehouses,
        inventory_snapshots=inventory_snapshots,
        demand_history=demand_history,
        purchase_orders=purchase_orders,
        product_classification=product_classification,
    )


if __name__ == "__main__":
    generate_all_tables()
