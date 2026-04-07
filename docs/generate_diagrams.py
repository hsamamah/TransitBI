"""
Generate architecture diagrams for the Seattle Transit DW project.

Usage:
    # First time setup:
    python3 -m venv docs/.venv
    docs/.venv/bin/pip install -r docs/requirements.txt

    # Generate all diagrams:
    docs/.venv/bin/python docs/generate_diagrams.py

Output: docs/images/*.png
Requires: graphviz system package (apt: graphviz / brew: graphviz / pacman: graphviz)
"""

import os
from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Glue, GlueDataCatalog, Quicksight, Redshift
from diagrams.aws.database import Dynamodb
from diagrams.aws.integration import Eventbridge, SNS
from diagrams.aws.management import Cloudwatch
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3

OUT = Path(__file__).parent / "images"
OUT.mkdir(exist_ok=True)

GRAPH_ATTR = {
    "fontsize": "14",
    "bgcolor": "white",
    "pad": "1.0",
    "splines": "ortho",
    "nodesep": "0.8",
    "ranksep": "1.2",
}

NODE_ATTR = {
    "fontsize": "12",
    "margin": "0.3",
}

CLUSTER_ATTR = {"fontsize": "13", "margin": "20"}


# ─────────────────────────────────────────────────────────────────────────────
# Diagram 1 — Full AWS Architecture
# ─────────────────────────────────────────────────────────────────────────────
def aws_architecture():
    with Diagram(
        "Seattle Transit DW — AWS Architecture",
        filename=str(OUT / "aws_architecture"),
        outformat="png",
        show=False,
        graph_attr=GRAPH_ATTR,
        node_attr=NODE_ATTR,
        direction="LR",
    ):
        with Cluster("Ingestion", graph_attr=CLUSTER_ATTR):
            eb_poll   = Eventbridge("EventBridge\n\n1-min schedule")
            lam_poll  = Lambda("gtfs-rt-polling")
            eb_daily  = Eventbridge("EventBridge\n\n08:00 PST")
            eb_static = Eventbridge("EventBridge\n\n07:00 PST")

        with Cluster("Storage", graph_attr=CLUSTER_ATTR):
            s3_raw     = S3("seattle-transit-raw\n\n.pb files · 90d retention")
            s3_staging = S3("seattle-transit-staging\n\nGTFS static + Glue scripts")

        with Cluster("RT Pipeline (Glue Workflow)", graph_attr=CLUSTER_ATTR):
            glue_rt      = Glue("gtfs-rt-parse-load")
            glue_inspect = Glue("transit-pipeline-inspector")
            ddb          = Dynamodb("DynamoDB\n\npipeline params")
            glue_stop    = Glue("factstop-skeleton\n& merge")
            glue_trip    = Glue("facttrip-skeleton\n& merge")
            glue_fsd     = Glue("factserviceday-load")

        with Cluster("Static Pipeline (Glue Workflow)", graph_attr=CLUSTER_ATTR):
            glue_ingest  = Glue("gtfs-static-ingestion")
            glue_crawler = GlueDataCatalog("gtfs-static-crawler")
            glue_valid   = Glue("gtfs-static-validation")
            glue_rsload  = Glue("gtfs-static-redshift-load")

        with Cluster("Data Warehouse", graph_attr=CLUSTER_ATTR):
            rs = Redshift("Redshift Serverless\n\nworkgroup: team  ·  db: dev")

        with Cluster("BI Layer", graph_attr=CLUSTER_ATTR):
            qs = Quicksight("QuickSight\n\n6 SPICE datasets\n1 dashboard")

        with Cluster("Alerting", graph_attr=CLUSTER_ATTR):
            eb_fail  = Eventbridge("EventBridge\n\njob failure rules")
            lam_fail = Lambda("transit-failure-notifier")
            sns_fail = SNS("transit-failure-alerts")
            cw       = Cloudwatch("CloudWatch Alarms\n\nLambda errors")

        # Ingestion flow
        eb_poll   >> lam_poll  >> s3_raw
        eb_daily  >> glue_rt
        eb_static >> glue_ingest

        # RT pipeline flow
        s3_raw    >> glue_rt
        glue_rt   >> glue_inspect
        glue_inspect >> ddb
        ddb       >> glue_stop
        ddb       >> glue_trip
        glue_stop >> glue_fsd
        glue_trip >> glue_fsd
        glue_stop >> rs
        glue_trip >> rs
        glue_fsd  >> rs

        # Static pipeline flow
        s3_staging >> glue_ingest
        glue_ingest  >> glue_crawler
        glue_crawler >> glue_valid
        glue_valid   >> glue_rsload
        glue_rsload  >> rs

        # BI
        rs >> qs

        # Alerting
        eb_fail  >> lam_fail >> sns_fail
        cw       >> sns_fail


# ─────────────────────────────────────────────────────────────────────────────
# Diagram 2 — Data Flow (raw feed → dashboard)
# ─────────────────────────────────────────────────────────────────────────────
def data_flow():
    with Diagram(
        "Seattle Transit DW — Data Flow",
        filename=str(OUT / "data_flow"),
        outformat="png",
        show=False,
        graph_attr={**GRAPH_ATTR, "splines": "curved"},
        node_attr=NODE_ATTR,
        direction="TB",
    ):
        with Cluster("External Feeds", graph_attr=CLUSTER_ATTR):
            oba   = Lambda("OneBusAway API\n\nGTFS-RT protobuf")
            gtfs  = Lambda("Sound Transit /\nKing County Metro\n\nGTFS Static ZIP")

        with Cluster("Raw / Staging (S3)", graph_attr=CLUSTER_ATTR):
            raw    = S3("seattle-transit-raw\n\ngtfs-rt/{agency}/{feed}/{ts}.pb")
            staged = S3("seattle-transit-staging\n\ngtfs-static/combined/{YYYY/MM/DD}/")

        with Cluster("Staging Schema (Redshift stg.*)", graph_attr=CLUSTER_ATTR):
            stg_rt     = Redshift("stg.rt_stop_time_updates\nstg.rt_vehicle_positions")
            stg_static = Redshift("stg.routes  ·  stg.trips\nstg.stops   ·  stg.stop_times\nstg.calendar  ·  ...")

        with Cluster("Dimension Tables (Redshift dw.*)", graph_attr=CLUSTER_ATTR):
            dims = Redshift("DimRoute  ·  DimStop  ·  DimTrip\nDimService  ·  DimDate  ·  DimTime\nDimAgency  ·  DimFeedVersion\nDimDirection  ·  DimShape")

        with Cluster("Fact Tables (Redshift dw.*)", graph_attr=CLUSTER_ATTR):
            facts = Redshift("FactStop\n(OTP · arrival deviation)\n\nFactTrip\n(trip status · VRM)\n\nFactServiceDay\n(daily aggregates)")

        with Cluster("BI Views (Redshift dw.*)", graph_attr=CLUSTER_ATTR):
            views = Redshift(
                "vw_otp_by_route_month\nvw_dailyvrm  ·  vw_dailyvrh\nv_missed_trip_rate_by_route\nv_routes_consistently_late\nvw_data_quality_daily  ·  ..."
            )

        qs = Quicksight("QuickSight SPICE\n\n6 datasets  ·  1 dashboard")

        oba   >> raw    >> stg_rt
        gtfs  >> staged >> stg_static
        stg_static >> dims
        stg_rt     >> Edge(label="merge") >> facts
        dims       >> facts
        facts      >> views
        views      >> qs


# ─────────────────────────────────────────────────────────────────────────────
# Diagram 3 — Failure Alerting Flow
# ─────────────────────────────────────────────────────────────────────────────
def alerting_flow():
    with Diagram(
        "Seattle Transit DW — Failure Alerting",
        filename=str(OUT / "alerting_flow"),
        outformat="png",
        show=False,
        graph_attr={**GRAPH_ATTR, "splines": "curved"},
        node_attr=NODE_ATTR,
        direction="LR",
    ):
        with Cluster("Failure Sources", graph_attr=CLUSTER_ATTR):
            glue_jobs = Glue("Any Glue Job\n\n→ FAILED")
            glue_wf   = Glue("Any Glue Workflow\n\n→ FAILED / STOPPED")
            lam_err   = Lambda("Lambda Functions\n\nerror metrics")

        with Cluster("Detection", graph_attr=CLUSTER_ATTR):
            eb_job  = Eventbridge("EventBridge\n\nGlue Job State Change")
            eb_wf   = Eventbridge("EventBridge\n\nGlue Workflow Run Status")
            cw_alm  = Cloudwatch("CloudWatch Alarms\n\n1+ errors / 5 min")

        with Cluster("Notification", graph_attr=CLUSTER_ATTR):
            lam_n = Lambda("transit-failure-notifier\n\nenriches with run details")
            sns   = SNS("transit-failure-alerts\n\nSNS Topic")

        email = Lambda("Team Email")

        glue_jobs >> eb_job  >> lam_n
        glue_wf   >> eb_wf   >> lam_n
        lam_err   >> cw_alm  >> sns
        lam_n     >> sns
        sns       >> email


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating diagrams → docs/images/")
    aws_architecture()
    print("  ✓ aws_architecture.png")
    data_flow()
    print("  ✓ data_flow.png")
    alerting_flow()
    print("  ✓ alerting_flow.png")
    print("Done.")
