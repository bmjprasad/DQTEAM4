"""
Data Quality Dashboard - Web-based UI for data quality management
Similar to Ataccama's dashboard interface
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.data_quality_engine import DataQualityEngine
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityDashboard").getOrCreate()

# Initialize data quality engine
dq_engine = DataQualityEngine(spark)

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Data Quality Dashboard"

# Sample data for demonstration
sample_data = {
    "datasets": [
        {"name": "customer_data", "table": "customers", "last_checked": "2024-01-15 10:30:00"},
        {"name": "sales_data", "table": "sales", "last_checked": "2024-01-15 09:45:00"},
        {"name": "product_data", "table": "products", "last_checked": "2024-01-15 11:15:00"}
    ],
    "quality_scores": [
        {"dataset": "customer_data", "score": 85, "grade": "B", "trend": "up"},
        {"dataset": "sales_data", "score": 92, "grade": "A", "trend": "stable"},
        {"dataset": "product_data", "score": 78, "grade": "C", "trend": "down"}
    ]
}

# Layout
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Data Quality Dashboard", className="text-center mb-4"),
            html.P("Comprehensive data quality monitoring and management", className="text-center text-muted")
        ])
    ]),
    
    # Navigation tabs
    dbc.Row([
        dbc.Col([
            dbc.Tabs([
                dbc.Tab(label="Overview", tab_id="overview"),
                dbc.Tab(label="Data Profiling", tab_id="profiling"),
                dbc.Tab(label="Validation Rules", tab_id="validation"),
                dbc.Tab(label="Quality Metrics", tab_id="metrics"),
                dbc.Tab(label="Anomaly Detection", tab_id="anomalies"),
                dbc.Tab(label="Reports", tab_id="reports")
            ], id="main-tabs", active_tab="overview")
        ])
    ], className="mb-4"),
    
    # Tab content
    dbc.Row([
        dbc.Col([
            html.Div(id="tab-content")
        ])
    ])
], fluid=True)

# Overview tab content
def create_overview_tab():
    return dbc.Container([
        # Quality score cards
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Overall Quality Score", className="card-title"),
                        html.H2("87", className="text-primary"),
                        html.P("Grade: B+", className="text-muted")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Datasets Monitored", className="card-title"),
                        html.H2("12", className="text-info"),
                        html.P("Active datasets", className="text-muted")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Rules Active", className="card-title"),
                        html.H2("45", className="text-success"),
                        html.P("Validation rules", className="text-muted")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Issues Found", className="card-title"),
                        html.H2("8", className="text-warning"),
                        html.P("Quality issues", className="text-muted")
                    ])
                ])
            ], width=3)
        ], className="mb-4"),
        
        # Quality trends chart
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Quality Score Trends"),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.line(
                                x=pd.date_range(start='2024-01-01', end='2024-01-15', freq='D'),
                                y=[85, 87, 86, 88, 89, 87, 90, 88, 89, 91, 90, 88, 87, 85, 87],
                                title="Quality Score Over Time"
                            ).update_layout(showlegend=False)
                        )
                    ])
                ])
            ], width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Recent Activities"),
                    dbc.CardBody([
                        html.Ul([
                            html.Li("Data profiling completed for customer_data"),
                            html.Li("Validation rule 'email_format' failed for sales_data"),
                            html.Li("Anomaly detected in product_data column 'price'"),
                            html.Li("Quality report generated for all datasets")
                        ])
                    ])
                ])
            ], width=4)
        ])
    ])

# Data profiling tab content
def create_profiling_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Data Profiling"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Select Dataset:"),
                                dcc.Dropdown(
                                    id="dataset-dropdown",
                                    options=[
                                        {"label": "Customer Data", "value": "customers"},
                                        {"label": "Sales Data", "value": "sales"},
                                        {"label": "Product Data", "value": "products"}
                                    ],
                                    value="customers"
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Sample Size:"),
                                dcc.Dropdown(
                                    id="sample-size-dropdown",
                                    options=[
                                        {"label": "Full Dataset", "value": "full"},
                                        {"label": "10% Sample", "value": "10"},
                                        {"label": "1% Sample", "value": "1"}
                                    ],
                                    value="10"
                                )
                            ], width=6)
                        ], className="mb-3"),
                        dbc.Button("Run Profiling", id="run-profiling-btn", color="primary", className="mb-3"),
                        html.Div(id="profiling-results")
                    ])
                ])
            ])
        ])
    ])

# Validation rules tab content
def create_validation_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Validation Rules"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.H5("Rule Configuration"),
                                dbc.Textarea(
                                    id="rules-textarea",
                                    placeholder="Enter validation rules in JSON format...",
                                    value=json.dumps({
                                        "not_null_rule": {
                                            "type": "not_null",
                                            "columns": ["id", "name", "email"]
                                        },
                                        "email_format_rule": {
                                            "type": "pattern",
                                            "column": "email",
                                            "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                                        }
                                    }, indent=2),
                                    rows=10
                                )
                            ], width=6),
                            dbc.Col([
                                html.H5("Rule Templates"),
                                dbc.ListGroup([
                                    dbc.ListGroupItem("Not Null Constraint", action=True),
                                    dbc.ListGroupItem("Email Format Validation", action=True),
                                    dbc.ListGroupItem("Phone Number Format", action=True),
                                    dbc.ListGroupItem("Range Validation", action=True),
                                    dbc.ListGroupItem("Unique Constraint", action=True)
                                ])
                            ], width=6)
                        ]),
                        dbc.Button("Run Validation", id="run-validation-btn", color="primary", className="mt-3"),
                        html.Div(id="validation-results")
                    ])
                ])
            ])
        ])
    ])

# Quality metrics tab content
def create_metrics_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Quality Metrics"),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.bar(
                                x=["Completeness", "Consistency", "Accuracy", "Validity", "Uniqueness"],
                                y=[85, 78, 92, 88, 75],
                                title="Quality Metrics Breakdown"
                            )
                        )
                    ])
                ])
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Quality Score Distribution"),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.histogram(
                                x=[85, 87, 86, 88, 89, 87, 90, 88, 89, 91, 90, 88, 87, 85, 87],
                                title="Quality Score Distribution"
                            )
                        )
                    ])
                ])
            ], width=6)
        ])
    ])

# Anomaly detection tab content
def create_anomalies_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Anomaly Detection"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Detection Method:"),
                                dcc.Dropdown(
                                    id="anomaly-method-dropdown",
                                    options=[
                                        {"label": "Statistical (IQR, Z-score)", "value": "statistical"},
                                        {"label": "Isolation Forest", "value": "isolation_forest"},
                                        {"label": "Clustering", "value": "clustering"}
                                    ],
                                    value="statistical"
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Threshold:"),
                                dcc.Slider(
                                    id="anomaly-threshold-slider",
                                    min=0.01,
                                    max=0.1,
                                    step=0.01,
                                    value=0.05,
                                    marks={i/100: f"{i}%" for i in range(1, 11, 2)}
                                )
                            ], width=6)
                        ], className="mb-3"),
                        dbc.Button("Detect Anomalies", id="detect-anomalies-btn", color="primary", className="mb-3"),
                        html.Div(id="anomaly-results")
                    ])
                ])
            ])
        ])
    ])

# Reports tab content
def create_reports_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Quality Reports"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.H5("Generate Report"),
                                dbc.Select(
                                    id="report-type-select",
                                    options=[
                                        {"label": "Comprehensive Quality Report", "value": "comprehensive"},
                                        {"label": "Validation Summary", "value": "validation"},
                                        {"label": "Anomaly Report", "value": "anomaly"},
                                        {"label": "Trend Analysis", "value": "trend"}
                                    ],
                                    value="comprehensive"
                                )
                            ], width=6),
                            dbc.Col([
                                html.H5("Export Format"),
                                dbc.Select(
                                    id="export-format-select",
                                    options=[
                                        {"label": "PDF", "value": "pdf"},
                                        {"label": "Excel", "value": "excel"},
                                        {"label": "JSON", "value": "json"}
                                    ],
                                    value="pdf"
                                )
                            ], width=6)
                        ], className="mb-3"),
                        dbc.Button("Generate Report", id="generate-report-btn", color="primary", className="mb-3"),
                        html.Div(id="report-results")
                    ])
                ])
            ])
        ])
    ])

# Callbacks
@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "active_tab")
)
def render_tab_content(active_tab):
    if active_tab == "overview":
        return create_overview_tab()
    elif active_tab == "profiling":
        return create_profiling_tab()
    elif active_tab == "validation":
        return create_validation_tab()
    elif active_tab == "metrics":
        return create_metrics_tab()
    elif active_tab == "anomalies":
        return create_anomalies_tab()
    elif active_tab == "reports":
        return create_reports_tab()
    else:
        return html.Div("Select a tab to view content")

@app.callback(
    Output("profiling-results", "children"),
    Input("run-profiling-btn", "n_clicks"),
    State("dataset-dropdown", "value"),
    State("sample-size-dropdown", "value")
)
def run_profiling(n_clicks, dataset, sample_size):
    if n_clicks is None:
        return ""
    
    try:
        # This is a placeholder - in practice, you'd load actual data
        return dbc.Alert("Profiling completed successfully! (This is a demo)", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

@app.callback(
    Output("validation-results", "children"),
    Input("run-validation-btn", "n_clicks"),
    State("rules-textarea", "value")
)
def run_validation(n_clicks, rules_json):
    if n_clicks is None:
        return ""
    
    try:
        rules = json.loads(rules_json)
        return dbc.Alert("Validation completed successfully! (This is a demo)", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

@app.callback(
    Output("anomaly-results", "children"),
    Input("detect-anomalies-btn", "n_clicks"),
    State("anomaly-method-dropdown", "value"),
    State("anomaly-threshold-slider", "value")
)
def detect_anomalies(n_clicks, method, threshold):
    if n_clicks is None:
        return ""
    
    try:
        return dbc.Alert(f"Anomaly detection completed using {method} method! (This is a demo)", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

@app.callback(
    Output("report-results", "children"),
    Input("generate-report-btn", "n_clicks"),
    State("report-type-select", "value"),
    State("export-format-select", "value")
)
def generate_report(n_clicks, report_type, export_format):
    if n_clicks is None:
        return ""
    
    try:
        return dbc.Alert(f"Report generated: {report_type} in {export_format} format! (This is a demo)", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)