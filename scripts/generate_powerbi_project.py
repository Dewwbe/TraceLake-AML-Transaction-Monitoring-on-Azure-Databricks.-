"""Generate the Power BI Project (powerbi/AML.pbip) from the table registry.

Output is a PBIP: a TMDL semantic model (Databricks Import-mode queries, relationships, DAX measures) and a
PBIR report (three pages). Open powerbi/AML.pbip in Power BI Desktop, set the host / HTTP path parameters
if they differ, and Refresh. Regenerate with:  python scripts/generate_powerbi_project.py
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from aml.schemas import TABLES  # noqa: E402

SCHEMA_BASE = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
DTYPE = {"string": "string", "timestamp": "dateTime", "date": "dateTime", "double": "double",
         "bigint": "int64", "int": "int64", "int64": "int64", "boolean": "boolean"}

# Columns left out of the model (arrays, hashes, normalised helper columns).
SKIP_COLS = {"dq_failed_rules", "row_hash", "name_norm"}


def t(n: int) -> str:
    return "\t" * n


def registry() -> dict:
    items = TABLES.values() if isinstance(TABLES, dict) else TABLES
    return {x.name: x for x in items}


# ---- table specs ----------------------------------------------------------------------------------------
# Each model table: (model name, source "schema.table", extra M steps, extra columns, calculated columns, measures)
# extra M steps receive/return the table in variable `t`.

def build_specs(reg: dict) -> list[dict]:
    def cols(src: str) -> list[tuple[str, str]]:
        return [(c.name, c.dtype) for c in reg[src].cols if c.name not in SKIP_COLS]

    specs = [
        dict(name="dim_date", src="gold.dim_date", key="date_key", date_table=True,
             measures=[]),
        dict(name="dim_rule", src="gold.dim_rule", key="rule_id", measures=[]),
        dict(name="dim_customer", src="gold.dim_customer", key="customer_key",
             extra_cols=[("risk_sort", "int64")],  # computed in M: a DAX sort column that reads its own base column is circular
             extra_m=['Table.AddColumn({p}, "risk_sort", each if [risk_rating] = "HIGH" then 1 else if [risk_rating] = "MEDIUM" then 2 else 3, Int64.Type)'],
             sort={"risk_rating": "risk_sort"},
             measures=[("Current Customers", "CALCULATE(COUNTROWS(dim_customer), dim_customer[is_current] = TRUE())", "#,0")]),
        dict(name="dim_account", src="gold.dim_account", key="account_id", measures=[]),
        dict(name="fact_transaction", src="gold.fact_transaction",
             measures=[("Transactions", "COUNTROWS(fact_transaction)", "#,0"),
                       ("Transaction Value (AED)", "SUM(fact_transaction[amount_aed])", "#,0"),
                       ("Labelled Laundering Transactions",
                        "CALCULATE(COUNTROWS(fact_transaction), fact_transaction[is_laundering] = 1)", "#,0")]),
        dict(name="fact_alert", src="gold.fact_alert",
             calc=[("Rule", "string", 'COALESCE(RELATED(dim_rule[rule_name]), "Sanctions screening")')],
             measures=[("Alerts", "COUNTROWS(fact_alert)", "#,0"),
                       ("Alerts per 1,000 Transactions", "DIVIDE([Alerts], [Transactions]) * 1000", "0.0")]),
        dict(name="fact_case", src="gold.fact_case",
             extra_cols=[("priority_sort", "int64")],
             extra_m=['Table.AddColumn({p}, "priority_sort", each if [priority] = "CRITICAL" then 1 else if [priority] = "HIGH" then 2 else if [priority] = "MEDIUM" then 3 else 4, Int64.Type)'],
             sort={"priority": "priority_sort"},
             measures=[("Cases", "COUNTROWS(fact_case)", "#,0"),
                       ("Escalated Cases",
                        'CALCULATE(COUNTROWS(fact_case), fact_case[disposition] = "ESCALATED")', "#,0"),
                       ("Cases In Review",
                        'CALCULATE(COUNTROWS(fact_case), fact_case[status] = "IN_REVIEW")', "#,0")]),
        dict(name="fact_screening", src="gold.fact_screening",
             measures=[("Screening Hits", "COUNTROWS(fact_screening)", "#,0")]),
        dict(name="rule_performance", src="gold.mart_rule_performance", latest="computed_at",
             measures=[("Rule Precision", "AVERAGE(rule_performance[precision])", "0.0%"),
                       ("Rule Recall", "AVERAGE(rule_performance[recall])", "0.0%")]),
        dict(name="quarantine", src="silver.transactions_quarantine",
             measures=[("Quarantined Transactions", "COUNTROWS(quarantine)", "#,0")]),
        dict(name="dq_failures", src="ops.dq_results", where="[rows_failed] > 0", measures=[]),
        dict(name="latest_pipeline_run", src="ops.pipeline_runs", latest="started_at",
             extra_cols=[("seconds", "double")],
             extra_m=['Table.AddColumn({p}, "seconds", each Duration.TotalSeconds([finished_at] - [started_at]), type number)'],
             measures=[]),
        dict(name="customer_rating_history", src="gold.dim_customer",
             select=["customer_id", "risk_rating", "risk_score", "valid_from", "valid_to", "is_current"],
             extra_cols=[("versions", "int64")],
             measures=[]),
    ]
    for s in specs:
        all_cols = cols(s["src"])
        if "select" in s:
            all_cols = [c for c in all_cols if c[0] in s["select"]]
        s["cols"] = all_cols
    return specs


# ---- TMDL -----------------------------------------------------------------------------------------------
def tmdl_table(s: dict) -> str:
    out = [f"table {s['name']}"]
    if s.get("date_table"):
        out.append(f"{t(1)}dataCategory: Time")
    out.append("")
    sort = s.get("sort", {})
    for name, typ in s["cols"] + s.get("extra_cols", []):
        out.append(f"{t(1)}column {name}")
        out.append(f"{t(2)}dataType: {DTYPE[typ]}")
        if s.get("date_table") and name == "date":
            out.append(f"{t(2)}isKey")
        if typ in ("date", "timestamp"):
            out.append(f"{t(2)}formatString: {'yyyy-mm-dd' if typ == 'date' else 'yyyy-mm-dd hh:nn'}")
        out.append(f"{t(2)}summarizeBy: none")
        out.append(f"{t(2)}sourceColumn: {name}")
        if name in sort:
            out.append(f"{t(2)}sortByColumn: {sort[name]}")
        out.append("")
    for name, typ, dax in s.get("calc", []):
        out.append(f"{t(1)}column {name} = {dax}")
        out.append(f"{t(2)}dataType: {DTYPE[typ]}")
        out.append(f"{t(2)}summarizeBy: none")
        out.append("")
    for name, dax, fmt in s.get("measures", []):
        q = f"'{name}'" if not name.isidentifier() else name
        out.append(f"{t(1)}measure {q} = {dax}")
        out.append(f"{t(2)}formatString: {fmt}")
        out.append("")
    schema, table = s["src"].split(".")
    steps: list[tuple[str, str]] = [("Source", f'fnTable("{schema}", "{table}")')]

    def add(expr: str) -> None:
        steps.append((f"Step{len(steps)}", expr.replace("{p}", steps[-1][0])))

    if "latest" in s:
        add('Table.SelectRows({p}, let r = Table.FirstN(Table.Sort({p}, {{"%s", Order.Descending}}), 1)[run_id]{0} in each [run_id] = r)'
            % s["latest"])
    if "where" in s:
        add(f"Table.SelectRows({{p}}, each {s['where']})")
    if s["name"] == "customer_rating_history":
        full = steps[-1][0]
        add('Table.Group({p}, {"customer_id"}, {{"versions", each Table.RowCount(_), Int64.Type}})')
        add("Table.SelectRows({p}, each [versions] > 1)")
        add(f'Table.NestedJoin({full}, {{"customer_id"}}, {{p}}, {{"customer_id"}}, "m", JoinKind.Inner)')
        add('Table.ExpandTableColumn({p}, "m", {"versions"})')
    elif "extra_m" in s:
        for e in s["extra_m"]:
            add(e)
    names = [c[0] for c in s["cols"]] + [c[0] for c in s.get("extra_cols", [])]
    add("Table.SelectColumns({p}, {" + ", ".join(f'"{n}"' for n in names) + "})")
    body = ",\n".join(f"    {n} = {e}" for n, e in steps)
    out.append(f"{t(1)}partition {s['name']} = m")
    out.append(f"{t(2)}mode: import")
    out.append(f"{t(2)}source =")
    out.append(f"{t(4)}let")
    out.extend(f"{t(4)}{line}" for line in body.splitlines())
    out.append(f"{t(4)}in")
    out.append(f"{t(5)}{steps[-1][0]}")
    out.append("")
    return "\n".join(out)


EXPRESSIONS = '''expression DatabricksHost = "{host}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

expression DatabricksHttpPath = "{path}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

expression Catalog = "aml" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

expression fnTable =
\t\t(schemaName as text, tableName as text) as table =>
\t\tlet
\t\t    Source = Databricks.Catalogs(DatabricksHost, DatabricksHttpPath, [Catalog = Catalog, Database = null, EnableAutomaticProxyDiscovery = null]),
\t\t    Db = Source{{[Name = Catalog, Kind = "Database"]}}[Data],
\t\t    Sch = Db{{[Name = schemaName, Kind = "Schema"]}}[Data],
\t\t    Tbl = Sch{{[Name = tableName, Kind = "Table"]}}[Data]
\t\tin
\t\t    Tbl
'''

RELATIONSHIPS = [
    ("fact_transaction", "date_key", "dim_date", "date_key", True),
    ("fact_transaction", "sender_account", "dim_account", "account_id", True),
    ("fact_transaction", "receiver_account", "dim_account", "account_id", False),
    ("fact_alert", "alert_date_key", "dim_date", "date_key", True),
    ("fact_alert", "rule_id", "dim_rule", "rule_id", True),
    ("fact_alert", "customer_key", "dim_customer", "customer_key", True),
    ("fact_alert", "case_id", "fact_case", "case_id", True),
]


def tmdl_relationships() -> str:
    out = []
    for ft, fc, tt, tc, active in RELATIONSHIPS:
        out.append(f"relationship {ft}_{fc}_to_{tt}")
        if not active:
            out.append(f"{t(1)}isActive: false")
        out.append(f"{t(1)}fromColumn: {ft}.{fc}")
        out.append(f"{t(1)}toColumn: {tt}.{tc}")
        out.append("")
    return "\n".join(out)


# ---- PBIR report ----------------------------------------------------------------------------------------
def col(table: str, prop: str) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": prop}}


def mea(table: str, prop: str) -> dict:
    return {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": prop}}


def proj(field: dict, active: bool = False) -> dict:
    kind = "Measure" if "Measure" in field else "Column"
    e = field[kind]
    p = {"field": field, "queryRef": f"{e['Expression']['SourceRef']['Entity']}.{e['Property']}",
         "nativeQueryRef": e["Property"]}
    if active:
        p["active"] = True
    return p


def visual(name: str, vtype: str, title: str, pos: tuple[int, int, int, int], roles: dict,
           sort: tuple[dict, str] | None = None) -> dict:
    x, y, w, h = pos
    query: dict = {"queryState": {r: {"projections": [proj(f, active=(r == "Category")) for f in fs]}
                                  for r, fs in roles.items()}}
    if sort:
        query["sortDefinition"] = {"sort": [{"field": sort[0], "direction": sort[1]}], "isDefaultSort": True}
    return {
        "$schema": f"{SCHEMA_BASE}/visualContainer/2.0.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "z": 0, "height": h, "width": w, "tabOrder": 0},
        "visual": {
            "visualType": vtype,
            "query": query,
            "visualContainerObjects": {"title": [{"properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "text": {"expr": {"Literal": {"Value": "'" + title.replace("'", "''") + "'"}}}}}]},
            "drillFilterOtherVisuals": True,
        },
    }


def pages() -> list[tuple[str, str, list[dict]]]:
    kpis = [("Transactions", "fact_transaction", "Transactions"), ("Alerts", "fact_alert", "Alerts"),
            ("Cases", "fact_case", "Cases"), ("Escalated cases", "fact_case", "Escalated Cases"),
            ("Sanctions screening hits", "fact_screening", "Screening Hits"),
            ("Quarantined transactions", "quarantine", "Quarantined Transactions")]
    overview = [visual(f"kpi{i}", "card", title, (20 + i * 205, 20, 195, 100), {"Values": [mea(tb, m)]})
                for i, (title, tb, m) in enumerate(kpis)]
    overview += [
        visual("alertsperday", "lineChart", "Alerts per day", (20, 140, 620, 270),
               {"Category": [col("dim_date", "date")], "Y": [mea("fact_alert", "Alerts")]}),
        visual("alertsbyrule", "clusteredBarChart", "Alerts by rule", (660, 140, 600, 270),
               {"Category": [col("fact_alert", "Rule")], "Y": [mea("fact_alert", "Alerts")]},
               sort=(mea("fact_alert", "Alerts"), "Descending")),
        visual("casesbypriority", "clusteredColumnChart", "Cases by priority", (20, 430, 300, 270),
               {"Category": [col("fact_case", "priority")], "Y": [mea("fact_case", "Cases")]}),
        visual("customersbyrisk", "clusteredColumnChart", "Current customer risk rating", (340, 430, 300, 270),
               {"Category": [col("dim_customer", "risk_rating")], "Y": [mea("dim_customer", "Current Customers")]}),
        visual("valuebypayment", "clusteredBarChart", "Transaction value by payment type (AED)", (660, 430, 600, 270),
               {"Category": [col("fact_transaction", "payment_type")],
                "Y": [mea("fact_transaction", "Transaction Value (AED)")]},
               sort=(mea("fact_transaction", "Transaction Value (AED)"), "Descending")),
    ]
    detection = [
        visual("ruleperf", "clusteredColumnChart", "Rule precision and recall (latest run)", (20, 20, 1240, 260),
               {"Category": [col("rule_performance", "rule_id")],
                "Y": [mea("rule_performance", "Rule Precision"), mea("rule_performance", "Rule Recall")]}),
        visual("topmatches", "tableEx", "Top sanctions matches", (20, 300, 620, 400),
               {"Values": [col("fact_screening", c) for c in
                           ("customer_name", "matched_name", "score", "band", "list_source")]},
               sort=(col("fact_screening", "score"), "Descending")),
        visual("ratingchanges", "tableEx", "Customers whose risk rating changed (SCD Type 2)", (660, 300, 600, 400),
               {"Values": [col("customer_rating_history", c) for c in
                           ("customer_id", "risk_rating", "risk_score", "valid_from", "valid_to", "is_current")]},
               sort=(col("customer_rating_history", "customer_id"), "Ascending")),
    ]
    health = [
        visual("dqfailures", "tableEx", "Data quality failures", (20, 20, 1240, 330),
               {"Values": [col("dq_failures", c) for c in
                           ("table_name", "rule_id", "rule_type", "severity", "action", "rows_checked",
                            "rows_failed", "fail_pct")]},
               sort=(col("dq_failures", "rows_failed"), "Descending")),
        visual("pipelinerun", "tableEx", "Latest pipeline run", (20, 370, 1240, 330),
               {"Values": [col("latest_pipeline_run", c) for c in ("stage", "status", "started_at", "seconds")]},
               sort=(col("latest_pipeline_run", "started_at"), "Ascending")),
    ]
    return [("overview", "AML Overview", overview), ("detection", "Detection quality", detection),
            ("health", "Pipeline health", health)]

THEME_FILE = "AMLPurple.json"


def theme() -> dict:
    """Dark purple theme: near-black violet page, lighter violet cards, violet / teal / pink data colours."""
    def solid(c: str) -> dict:
        return {"solid": {"color": c}}

    text, muted = "#EDE3FF", "#B9A5DD"
    return {
        "name": "AML Purple",
        "dataColors": ["#B14CFF", "#19D3C5", "#FF4FA3", "#FFB03B", "#6C8CFF", "#7DE36B", "#D2B2FF", "#FF7A59"],
        "background": "#2B1250", "foreground": text, "tableAccent": "#B14CFF",
        "good": "#19D3C5", "neutral": "#FFB03B", "bad": "#FF4FA3",
        "maximum": "#B14CFF", "center": "#6C3BC4", "minimum": "#2E1658",
        "textClasses": {
            "callout": {"fontSize": 28, "fontFace": "Segoe UI Semibold", "color": "#FFFFFF"},
            "title": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": text},
            "header": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": text},
            "label": {"fontSize": 10, "fontFace": "Segoe UI", "color": muted},
        },
        "visualStyles": {
            "*": {"*": {
                "background": [{"show": True, "color": solid("#2B1250"), "transparency": 0}],
                "border": [{"show": True, "color": solid("#4A2790"), "radius": 10}],
                "title": [{"show": True, "fontColor": solid(text), "fontSize": 12}],
                "visualHeader": [{"show": False}],
                "categoryAxis": [{"labelColor": solid(muted)}],
                "valueAxis": [{"labelColor": solid(muted), "gridlineColor": solid("#43237F")}],
                "legend": [{"labelColor": solid(muted)}],
                "columnHeaders": [{"fontColor": solid("#FFFFFF"), "backColor": solid("#4A2790")}],
                "values": [{"fontColor": solid(text), "backColor": solid("#2B1250"),
                            "backColorSecondary": solid("#34175E")}],
            }},
            "page": {"*": {
                "background": [{"color": solid("#150824"), "transparency": 0}],
                "outspace": [{"color": solid("#0E0518")}],
            }},
        },
    }


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def write_json(path: Path, obj: dict) -> None:
    write(path, json.dumps(obj, indent=2) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="powerbi")
    ap.add_argument("--host", default="<your-workspace>.azuredatabricks.net", help="Databricks workspace hostname (no https://); default is a placeholder to set in Power BI")
    ap.add_argument("--http-path", default="/sql/1.0/warehouses/<warehouse-id>", help="SQL warehouse HTTP path; default is a placeholder to set in Power BI")
    args = ap.parse_args()
    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)

    specs = build_specs(registry())
    sm = out / "AML.SemanticModel"
    write_json(sm / "definition.pbism", {"version": "4.2", "settings": {}})
    write(sm / "definition" / "database.tmdl", "database\n\tcompatibilityLevel: 1600\n")
    names = "\n".join(f"ref table {s['name']}" for s in specs)
    write(sm / "definition" / "model.tmdl",
          "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
          "\tdiscourageImplicitMeasures\n\tsourceQueryCulture: en-US\n\n" + names + "\n"
          "\nref expression DatabricksHost\nref expression DatabricksHttpPath\nref expression Catalog\n"
          "ref expression fnTable\n")
    write(sm / "definition" / "expressions.tmdl",
          EXPRESSIONS.format(host=args.host, path=args.http_path))
    write(sm / "definition" / "relationships.tmdl", tmdl_relationships())
    for s in specs:
        write(sm / "definition" / "tables" / f"{s['name']}.tmdl", tmdl_table(s))

    rp = out / "AML.Report"
    write_json(rp / "definition.pbir", {"version": "4.0", "datasetReference": {"byPath": {"path": "../AML.SemanticModel"}}})
    d = rp / "definition"
    write_json(d / "version.json", {"$schema": f"{SCHEMA_BASE}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    write_json(d / "report.json", {
        "$schema": f"{SCHEMA_BASE}/report/2.0.0/schema.json",
        "themeCollection": {
            "baseTheme": {"name": "CY24SU10", "reportVersionAtImport": "5.60", "type": "SharedResources"},
            "customTheme": {"name": THEME_FILE, "reportVersionAtImport": "5.60", "type": "RegisteredResources"},
        },
        "resourcePackages": [{
            "name": "RegisteredResources", "type": "RegisteredResources",
            "items": [{"name": THEME_FILE, "path": THEME_FILE, "type": "CustomTheme"}],
        }],
    })
    write_json(rp / "StaticResources" / "RegisteredResources" / THEME_FILE, theme())
    pg = pages()
    write_json(d / "pages" / "pages.json", {
        "$schema": f"{SCHEMA_BASE}/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p[0] for p in pg], "activePageName": pg[0][0]})
    for pname, display, visuals in pg:
        write_json(d / "pages" / pname / "page.json", {
            "$schema": f"{SCHEMA_BASE}/page/2.0.0/schema.json", "name": pname, "displayName": display,
            "displayOption": "FitToPage", "height": 720, "width": 1280})
        for v in visuals:
            write_json(d / "pages" / pname / "visuals" / v["name"] / "visual.json", v)

    write_json(out / "AML.pbip", {"version": "1.0", "artifacts": [{"report": {"path": "AML.Report"}}],
                                   "settings": {"enableAutoRecovery": True}})
    write(out / ".gitignore", "**/.pbi/localSettings.json\n**/.pbi/cache.abf\n")
    print(f"Wrote {out}/AML.pbip ({len(specs)} tables, {len(RELATIONSHIPS)} relationships, "
          f"{sum(len(p[2]) for p in pg)} visuals)")


if __name__ == "__main__":
    main()
