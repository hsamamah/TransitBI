"""
Create the VOMS (Vehicles Operated in Maximum Service) analysis in QuickSight.

Run:
    python3 quicksight/create_voms_analysis.py

Creates or updates analysis ID: a1b2c3d4-voms-0001-0000-transit-bi-dw
in the Seattle Transit DW shared folder.

Dataset: v_voms (f866df43-50a4-492d-92f1-14901dee795d)
Columns used:
  monthly_voms     INTEGER  — vehicles in max service this month
  ntd_period_voms  INTEGER  — vehicles for NTD reporting period
  agencyname       STRING   — King County Metro / Sound Transit
  mode             STRING   — bus, rail, etc.
  fulldate         DATETIME — first day of each month
  peak_period      STRING   — AM Peak / PM Peak / Base
  calendaryear     INTEGER  — year
  calendarmonth    INTEGER  — month number
"""

import json
import boto3

ACCOUNT        = "805699509606"
REGION         = "us-west-2"
ANALYSIS_ID    = "a1b2c3d4-voms-0001-0000-transit-bi-dw"
ANALYSIS_NAME  = "VOMS — Vehicles Operated in Maximum Service"
DATASET_ID     = "f866df43-50a4-492d-92f1-14901dee795d"
DATASET_ARN    = f"arn:aws:quicksight:{REGION}:{ACCOUNT}:dataset/{DATASET_ID}"
DATASET_IDENT  = "v_voms"
FOLDER_ID      = "240636fa-ade1-4f5a-9929-67acda51d579"
QS_PRINCIPAL   = f"arn:aws:quicksight:{REGION}:{ACCOUNT}:user/default/hani-admin"

qs = boto3.client("quicksight", region_name=REGION)

# ── Helpers ───────────────────────────────────────────────────────────────────
def col(column_name):
    return {"DataSetIdentifier": DATASET_IDENT, "ColumnName": column_name}

def num_measure(field_id, column_name, agg="SUM"):
    return {
        "NumericalMeasureField": {
            "FieldId": field_id,
            "Column": col(column_name),
            "AggregationFunction": {"SimpleNumericalAggregation": agg},
        }
    }

def cat_dim(field_id, column_name):
    return {
        "CategoricalDimensionField": {
            "FieldId": field_id,
            "Column": col(column_name),
        }
    }

def date_dim(field_id, column_name, granularity="MONTH", hierarchy_id=None):
    d = {
        "DateDimensionField": {
            "FieldId": field_id,
            "Column": col(column_name),
            "DateGranularity": granularity,
        }
    }
    if hierarchy_id:
        d["DateDimensionField"]["HierarchyId"] = hierarchy_id
    return d

def kpi_visual(visual_id, title, value_field):
    trend_field_id = f"{visual_id}-trend"
    return {
        "KPIVisual": {
            "VisualId": visual_id,
            "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": title}},
            "Subtitle": {"Visibility": "VISIBLE"},
            "ChartConfiguration": {
                "FieldWells": {
                    "Values": [value_field],
                    "TargetValues": [],
                    "TrendGroups": [date_dim(trend_field_id, "fulldate", hierarchy_id=trend_field_id)],
                },
                "SortConfiguration": {},
                "KPIOptions": {
                    "Comparison": {"ComparisonMethod": "PERCENT_DIFFERENCE"},
                    "PrimaryValueDisplayType": "ACTUAL",
                    "Sparkline": {"Visibility": "VISIBLE", "Type": "AREA"},
                    "VisualLayoutOptions": {"StandardLayout": {"Type": "VERTICAL"}},
                },
            },
            "Actions": [],
            "ColumnHierarchies": [
                {
                    "DateTimeHierarchy": {
                        "HierarchyId": trend_field_id,
                        "DrillDownFilters": [],
                    }
                }
            ],
        }
    }


# ── Visuals ───────────────────────────────────────────────────────────────────

# 1. KPI — Monthly VOMS
kpi_monthly = kpi_visual(
    "voms-kpi-monthly",
    "Monthly VOMS",
    num_measure("voms-kpi-monthly-val", "monthly_voms"),
)

# 2. KPI — NTD Period VOMS
kpi_ntd = kpi_visual(
    "voms-kpi-ntd",
    "NTD Period VOMS",
    num_measure("voms-kpi-ntd-val", "ntd_period_voms"),
)

# 3. Line chart — Monthly VOMS trend by agency over time
line_trend = {
    "LineChartVisual": {
        "VisualId": "voms-line-trend",
        "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "Monthly VOMS by Agency"}},
        "Subtitle": {"Visibility": "VISIBLE"},
        "ChartConfiguration": {
            "FieldWells": {
                "LineChartAggregatedFieldWells": {
                    "Category": [date_dim("voms-line-x", "fulldate", "MONTH", hierarchy_id="voms-line-x")],
                    "Values": [num_measure("voms-line-y", "monthly_voms")],
                    "Colors": [cat_dim("voms-line-color", "agencyname")],
                }
            },
            "SortConfiguration": {
                "CategorySort": [
                    {
                        "FieldSort": {
                            "FieldId": "voms-line-x",
                            "Direction": "ASC",
                        }
                    }
                ]
            },
            "Type": "LINE",
            "XAxisDisplayOptions": {"AxisOffset": "0px"},
            "DataLabels": {"Visibility": "HIDDEN"},
            "Legend": {"Visibility": "VISIBLE", "Position": "BOTTOM"},
            "Tooltip": {"TooltipVisibility": "VISIBLE", "SelectedTooltipType": "DETAILED"},
        },
        "Actions": [],
        "ColumnHierarchies": [
            {"DateTimeHierarchy": {"HierarchyId": "voms-line-x", "DrillDownFilters": []}}
        ],
    }
}

# 4. Bar chart — VOMS by mode
bar_mode = {
    "BarChartVisual": {
        "VisualId": "voms-bar-mode",
        "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "VOMS by Mode"}},
        "Subtitle": {"Visibility": "VISIBLE"},
        "ChartConfiguration": {
            "FieldWells": {
                "BarChartAggregatedFieldWells": {
                    "Category": [cat_dim("voms-bar-cat", "mode")],
                    "Values": [num_measure("voms-bar-val", "monthly_voms")],
                    "Colors": [cat_dim("voms-bar-color", "agencyname")],
                    "SmallMultiples": [],
                }
            },
            "SortConfiguration": {
                "CategorySort": [
                    {
                        "FieldSort": {
                            "FieldId": "voms-bar-val",
                            "Direction": "DESC",
                        }
                    }
                ],
                "CategoryItemsLimit": {"OtherCategories": "INCLUDE"},
                "ColorItemsLimit": {"OtherCategories": "INCLUDE"},
            },
            "Orientation": "HORIZONTAL",
            "DataLabels": {"Visibility": "VISIBLE", "Overlap": "DISABLE_OVERLAP"},
            "Legend": {"Visibility": "VISIBLE", "Position": "BOTTOM"},
            "Tooltip": {"TooltipVisibility": "VISIBLE", "SelectedTooltipType": "DETAILED"},
        },
        "Actions": [],
        "ColumnHierarchies": [],
    }
}

# 5. Bar chart — VOMS by peak period
bar_peak = {
    "BarChartVisual": {
        "VisualId": "voms-bar-peak",
        "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "VOMS by Peak Period"}},
        "Subtitle": {"Visibility": "VISIBLE"},
        "ChartConfiguration": {
            "FieldWells": {
                "BarChartAggregatedFieldWells": {
                    "Category": [cat_dim("voms-peak-cat", "peak_period")],
                    "Values": [num_measure("voms-peak-val", "monthly_voms")],
                    "Colors": [],
                    "SmallMultiples": [],
                }
            },
            "SortConfiguration": {
                "CategorySort": [
                    {
                        "FieldSort": {
                            "FieldId": "voms-peak-val",
                            "Direction": "DESC",
                        }
                    }
                ],
                "CategoryItemsLimit": {"OtherCategories": "INCLUDE"},
                "ColorItemsLimit": {"OtherCategories": "INCLUDE"},
            },
            "Orientation": "VERTICAL",
            "DataLabels": {"Visibility": "VISIBLE", "Overlap": "DISABLE_OVERLAP"},
            "Legend": {"Visibility": "HIDDEN"},
            "Tooltip": {"TooltipVisibility": "VISIBLE", "SelectedTooltipType": "DETAILED"},
        },
        "Actions": [],
        "ColumnHierarchies": [],
    }
}

# 6. Table — Detailed breakdown
table_detail = {
    "TableVisual": {
        "VisualId": "voms-table-detail",
        "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "VOMS Detail — Agency / Mode / Month"}},
        "Subtitle": {"Visibility": "VISIBLE"},
        "ChartConfiguration": {
            "FieldWells": {
                "TableAggregatedFieldWells": {
                    "GroupBy": [
                        date_dim("voms-tbl-date", "fulldate", "MONTH"),
                        cat_dim("voms-tbl-agency", "agencyname"),
                        cat_dim("voms-tbl-mode", "mode"),
                        cat_dim("voms-tbl-peak", "peak_period"),
                    ],
                    "Values": [
                        num_measure("voms-tbl-monthly", "monthly_voms"),
                        num_measure("voms-tbl-ntd", "ntd_period_voms"),
                    ],
                }
            },
            "SortConfiguration": {
                "RowSort": [
                    {"FieldSort": {"FieldId": "voms-tbl-date", "Direction": "DESC"}}
                ]
            },
            "TableOptions": {
                "HeaderStyle": {"TextWrap": "WRAP", "Height": 25},
                "CellStyle": {"Height": 25},
                "Orientation": "VERTICAL",
            },
            "PaginatedReportOptions": {"VerticalOverflowVisibility": "VISIBLE"},
        },
        "Actions": [],
    }
}

# ── Layout (grid: 2 KPIs top, line chart, 2 bars, table) ─────────────────────
# QuickSight free-form grid: 1 unit = ~20px. Canvas is ~1620 wide × 900+ tall.
LAYOUT_ELEMENTS = [
    # KPI Monthly — top left
    {"ElementId": "voms-kpi-monthly", "ElementType": "VISUAL",
     "ColumnIndex": 0,  "ColumnSpan": 9,  "RowIndex": 0, "RowSpan": 7},
    # KPI NTD — top right of KPIs
    {"ElementId": "voms-kpi-ntd",     "ElementType": "VISUAL",
     "ColumnIndex": 9,  "ColumnSpan": 9,  "RowIndex": 0, "RowSpan": 7},
    # Line chart — full width row 2
    {"ElementId": "voms-line-trend",  "ElementType": "VISUAL",
     "ColumnIndex": 0,  "ColumnSpan": 36, "RowIndex": 7, "RowSpan": 14},
    # Bar mode — left half row 3
    {"ElementId": "voms-bar-mode",    "ElementType": "VISUAL",
     "ColumnIndex": 0,  "ColumnSpan": 18, "RowIndex": 21, "RowSpan": 12},
    # Bar peak — right half row 3
    {"ElementId": "voms-bar-peak",    "ElementType": "VISUAL",
     "ColumnIndex": 18, "ColumnSpan": 18, "RowIndex": 21, "RowSpan": 12},
    # Table — full width row 4
    {"ElementId": "voms-table-detail","ElementType": "VISUAL",
     "ColumnIndex": 0,  "ColumnSpan": 36, "RowIndex": 33, "RowSpan": 15},
]

# ── Analysis definition ───────────────────────────────────────────────────────
DEFINITION = {
    "DataSetIdentifierDeclarations": [
        {"Identifier": DATASET_IDENT, "DataSetArn": DATASET_ARN}
    ],
    "Sheets": [
        {
            "SheetId": "voms-sheet-01",
            "Name": "VOMS Overview",
            "Visuals": [kpi_monthly, kpi_ntd, line_trend, bar_mode, bar_peak, table_detail],
            "Layouts": [
                {
                    "Configuration": {
                        "GridLayout": {
                            "Elements": LAYOUT_ELEMENTS,
                            "CanvasSizeOptions": {
                                "ScreenCanvasSizeOptions": {
                                    "ResizeOption": "RESPONSIVE",
                                }
                            },
                        }
                    }
                }
            ],
            "ContentType": "INTERACTIVE",
        }
    ],
    "CalculatedFields": [],
    "ParameterDeclarations": [],
    "FilterGroups": [],
    "ColumnConfigurations": [],
    "AnalysisDefaults": {
        "DefaultNewSheetConfiguration": {
            "InteractiveLayoutConfiguration": {
                "Grid": {
                    "CanvasSizeOptions": {
                        "ScreenCanvasSizeOptions": {
                            "ResizeOption": "RESPONSIVE",
                        }
                    }
                }
            },
            "SheetContentType": "INTERACTIVE",
        }
    },
}

PERMISSIONS = [
    {
        "Principal": QS_PRINCIPAL,
        "Actions": [
            "quicksight:RestoreAnalysis",
            "quicksight:UpdateAnalysisPermissions",
            "quicksight:DeleteAnalysis",
            "quicksight:DescribeAnalysisPermissions",
            "quicksight:QueryAnalysis",
            "quicksight:DescribeAnalysis",
            "quicksight:UpdateAnalysis",
        ],
    }
]


# ── Create or update ──────────────────────────────────────────────────────────
def analysis_exists():
    try:
        qs.describe_analysis(AwsAccountId=ACCOUNT, AnalysisId=ANALYSIS_ID)
        return True
    except qs.exceptions.ResourceNotFoundException:
        return False


def add_to_folder(analysis_id):
    try:
        qs.create_folder_membership(
            AwsAccountId=ACCOUNT,
            FolderId=FOLDER_ID,
            MemberId=analysis_id,
            MemberType="ANALYSIS",
        )
        print(f"  ✓ Added to shared folder")
    except qs.exceptions.ResourceExistsException:
        print(f"  ✓ Already in shared folder")


if __name__ == "__main__":
    print(f"Analysis ID : {ANALYSIS_ID}")
    print(f"Analysis    : {ANALYSIS_NAME}")
    print(f"Dataset     : {DATASET_IDENT} ({DATASET_ID})")
    print()

    if analysis_exists():
        print("Updating existing analysis...")
        resp = qs.update_analysis(
            AwsAccountId=ACCOUNT,
            AnalysisId=ANALYSIS_ID,
            Name=ANALYSIS_NAME,
            Definition=DEFINITION,
        )
        print(f"  ✓ Updated — status: {resp.get('UpdateStatus', 'UNKNOWN')}")
    else:
        print("Creating new analysis...")
        resp = qs.create_analysis(
            AwsAccountId=ACCOUNT,
            AnalysisId=ANALYSIS_ID,
            Name=ANALYSIS_NAME,
            Definition=DEFINITION,
            Permissions=PERMISSIONS,
        )
        print(f"  ✓ Created — status: {resp.get('CreationStatus', 'UNKNOWN')}")

    add_to_folder(ANALYSIS_ID)

    print()
    print("Run to export and commit:")
    print("  bash deploy/deploy_quicksight.sh --export")
    print(f"  git add quicksight/analyses/{ANALYSIS_ID}.json && git commit")
