"""A fake DAX engine for the BDD tests - the prototype's offline engine, wired
in via ``set_query_engine`` so the whole suite runs with no Fabric capacity and
no network."""

from __future__ import annotations

import pandas as pd


def fake_engine(dataset, dax, workspace=None, impersonate=None):
    d = " ".join(dax.split())

    if "INFO.MEASURES" in d or "TMSCHEMA_MEASURES" in d:
        return pd.DataFrame({"[Name]": ["Total Sales", "Margin %", "Order Count"]})
    if "INFO.TABLES" in d or "TMSCHEMA_TABLES" in d:
        return pd.DataFrame({"[Name]": ["Sales", "Date", "Product", "Store"]})

    if 'ROW("x", 1)' in d or 'ROW("x",1)' in d:
        return pd.DataFrame({"[x]": [1]})

    if "Average Order Value" in d:
        return pd.DataFrame(
            {
                "Date[Fiscal Year]": ["FY2021", "FY2022", "FY2023", "FY2024", "FY2025"],
                "[Orders]": [10, 20, 30, 40, 50],
                "[Average Order Value]": [1.5, 2.5, 3.5, 4.5, 5.5],
            }
        )

    grouped = "SUMMARIZECOLUMNS" in d
    if grouped and "Product'[Category]" in d:
        return pd.DataFrame(
            {
                "Product[Category]": ["Bikes", "Accessories", "Clothing"],
                "[Total Sales]": [8102110.1132, 2880194.1801, 1495000.0],
            }
        )
    if grouped and "Store'[Region]" in d:
        if impersonate:
            return pd.DataFrame({"Store[Region]": ["North"], "[Total Sales]": [4201880.02]})
        return pd.DataFrame(
            {
                "Store[Region]": ["North", "South", "West"],
                "[Total Sales]": [4201880.02, 3995110.55, 4280313.72],
            }
        )

    if "Sales YoY %" in d:
        return pd.DataFrame({"[Sales YoY %]": [None]})
    if "Margin %" in d:
        return pd.DataFrame({"[Margin %]": [0.4213]})
    if "Total Sales" in d:
        value = 12477304.2871
        for region, v in (("North", 4201880.02), ("South", 3995110.55), ("West", 4280313.72)):
            if f'"{region}"' in d:
                value = v
        return pd.DataFrame({"[Total Sales]": [value]})

    raise RuntimeError(f"fake_engine: unhandled query: {d[:160]}")


def exploding_engine(*args, **kwargs):
    raise AssertionError("the query engine must not be called")
