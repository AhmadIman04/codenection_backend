import os
from dotenv import load_dotenv
from fastapi import FastAPI, Form, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse , JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from supabase import create_client
from dateutil.relativedelta import relativedelta
from supabase import create_client, Client

from datetime import datetime

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import List
import pandas as pd



load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")  # returns None if not set
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # raises KeyError if not set
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)





@app.get("/active_reports_count_total")
def get_active_reports_count():
    try:
        # query Incident_Reports where Active == "active"
        response = (
            supabase.table("Incident_Reports")
            .select("id", count="exact")
            .eq("Active", "Active")
            .execute()
        )

        # response.count holds the number of rows
        return {"active_reports_count": response.count}

    except Exception as e:
        return {"error": str(e)}

@app.get("/average_response_time_total")
def get_average_response_time():
    try:
        # fetch only the time_taken_to_solve column
        response = (
            supabase.table("Incident_Reports")
            .select("time_taken_to_solve")
            .execute()
        )

        times = [row["time_taken_to_solve"] for row in response.data if row.get("time_taken_to_solve") is not None]

        if not times:
            return {"average_response_time": 0}

        avg_time = sum(times) / len(times)
        return {"average_response_time": round(avg_time,2)}

    except Exception as e:
        return {"error": str(e)}


from datetime import datetime

@app.get("/number_reports_latest_month")
def get_number_reports_latest_month():
    try:
        # Fetch all reports
        response = supabase.table("Incident_Reports").select("*").execute()
        df = pd.DataFrame(response.data)

        # Convert Date_of_report from string -> datetime
        df["Date_of_report"] = pd.to_datetime(
            df["Date_of_report"], dayfirst=True, errors="coerce"
        )

        if df["Date_of_report"].isnull().all():
            return {"error": "No valid dates found in Date_of_report column"}

        # Find latest month & year in dataset
        latest_date = df["Date_of_report"].max()
        latest_month = latest_date.month
        latest_year = latest_date.year

        # Filter rows that match latest month & year
        df_latest = df[
            (df["Date_of_report"].dt.month == latest_month)
            & (df["Date_of_report"].dt.year == latest_year)
        ]

        # Count number of reports in latest month
        num_reports = len(df_latest)

        return {"number_reports_latest_month": num_reports}

    except Exception as e:
        return {"error": str(e)}




@app.get("/resolution_rate_total")
def get_resolution_rate():
    try:
        # Get total number of reports
        total_response = (
            supabase.table("Incident_Reports")
            .select("id", count="exact")
            .execute()
        )

        total_reports = total_response.count or 0

        if total_reports == 0:
            return {"resolution_rate": 0}

        # Get total solved reports
        solved_response = (
            supabase.table("Incident_Reports")
            .select("id", count="exact")
            .eq("Active", "solved")
            .execute()
        )

        solved_reports = solved_response.count or 0

        # Calculate resolution rate percentage
        resolution_rate = (solved_reports / total_reports) * 100

        return {
            "total_reports": total_reports,
            "solved_reports": solved_reports,
            "resolution_rate": round(resolution_rate, 2)  # keep 2 decimal places
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/recent_reports")
def get_recent_reports():
    try:
        # Fetch all reports
        response = supabase.table("Incident_Reports").select("*").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"recent_reports": []}

        # Convert to datetime (dayfirst=True since your format is D/M/YYYY)
        df["Date_of_report"] = pd.to_datetime(
            df["Date_of_report"], dayfirst=True, errors="coerce"
        )

        # Drop rows with invalid dates
        df = df.dropna(subset=["Date_of_report"])

        # Sort by date (latest first) and take top 4
        df_sorted = df.sort_values(by="Date_of_report", ascending=False).head(4)

        # Convert back to dict for JSON response
        recent_reports = df_sorted.to_dict(orient="records")

        return {"recent_reports": recent_reports}

    except Exception as e:
        return {"error": str(e)}



@app.get("/all_reports")
def get_all_reports(status_filter :str, types_filter:str):
    try:
        response = (
            supabase.table("Incident_Reports")
            .select("*")
            .execute()
        )

        df = pd.DataFrame(response.data)

        if(status_filter != "All Status"):
            df = df[df["Active"]==status_filter]

        if(types_filter != "All Types"):
            df = df[df["Types_of_report"]==types_filter]

        return {"all_reports": df.to_dict(orient="records")}

    except Exception as e:
        return {"error": str(e)}


@app.get("/unique_values")
def get_unique_values():
    try:
        response = (
            supabase.table("Incident_Reports")
            .select("Active, Types_of_report")
            .execute()
        )

        df = pd.DataFrame(response.data)

        active_values = df["Active"].dropna().unique().tolist()
        types_values = df["Types_of_report"].dropna().unique().tolist()

        return {
            "unique_active_values": active_values,
            "unique_types_values": types_values
        }

    except Exception as e:
        return {"error": str(e)}



@app.get("/active_reports_count_latest")
def get_active_reports_count_latest():
    try:
        # Fetch all reports (need dates to filter latest month)
        response = supabase.table("Incident_Reports").select("*").execute()
        df = pd.DataFrame(response.data)

        # Convert Date_of_report into datetime
        df["Date_of_report"] = pd.to_datetime(
            df["Date_of_report"], dayfirst=True, errors="coerce"
        )

        if df["Date_of_report"].isnull().all():
            return {"error": "No valid dates found in Date_of_report column"}

        # Find latest month & year
        latest_date = df["Date_of_report"].max()
        latest_month = latest_date.month
        latest_year = latest_date.year

        # Filter only latest month & Active == "Active"
        df_latest = df[
            (df["Date_of_report"].dt.month == latest_month)
            & (df["Date_of_report"].dt.year == latest_year)
            & (df["Active"] == "Active")
        ]

        return {"active_reports_count_latest": len(df_latest)}

    except Exception as e:
        return {"error": str(e)}


@app.get("/emergency_reports_total")
def get_emergency_reports_total():
    try:
        # Fetch only the emergency_type column
        response = supabase.table("Incident_Reports").select("emergency_type").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"emergency_reports_total": 0}

        # Count rows where emergency_type == "Emergency"
        emergency_count = (df["emergency_type"] == "Emergency").sum()

        return {"emergency_reports_total": int(emergency_count)}

    except Exception as e:
        return {"error": str(e)}



@app.get("/emergency_reports_latest_month")
def get_emergency_reports_latest_month():
    try:
        # Fetch relevant columns
        response = supabase.table("Incident_Reports").select("Date_of_report, emergency_type").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"emergency_reports_latest_month": 0}

        # Convert Date_of_report to datetime
        df["Date_of_report"] = pd.to_datetime(df["Date_of_report"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["Date_of_report"])  # remove invalid dates

        # Find latest month & year in dataset
        latest_date = df["Date_of_report"].max()
        latest_month = latest_date.month
        latest_year = latest_date.year

        # Filter for latest month and emergency only
        df_latest = df[
            (df["Date_of_report"].dt.month == latest_month) &
            (df["Date_of_report"].dt.year == latest_year) &
            (df["emergency_type"] == "Emergency")
        ]

        return {"emergency_reports_latest_month": len(df_latest)}

    except Exception as e:
        return {"error": str(e)}


@app.get("/average_response_time_latest")
def get_average_response_time_latest():
    try:
        # Fetch all reports (need Date_of_report + time_taken_to_solve)
        response = supabase.table("Incident_Reports").select("*").execute()
        df = pd.DataFrame(response.data)

        # Convert dates
        df["Date_of_report"] = pd.to_datetime(
            df["Date_of_report"], dayfirst=True, errors="coerce"
        )

        if df["Date_of_report"].isnull().all():
            return {"error": "No valid dates found in Date_of_report column"}

        # Find latest month/year
        latest_date = df["Date_of_report"].max()
        latest_month, latest_year = latest_date.month, latest_date.year

        # Filter rows
        df_latest = df[
            (df["Date_of_report"].dt.month == latest_month)
            & (df["Date_of_report"].dt.year == latest_year)
        ]

        times = df_latest["time_taken_to_solve"].dropna().astype(float)

        if times.empty:
            return {"average_response_time_latest": 0}

        avg_time = times.mean()
        return {"average_response_time_latest": round(avg_time, 2)}

    except Exception as e:
        return {"error": str(e)}

@app.get("/resolution_rate_latest")
def get_resolution_rate_latest():
    try:
        # Fetch all reports (need Active + Date_of_report)
        response = supabase.table("Incident_Reports").select("*").execute()
        df = pd.DataFrame(response.data)

        # Convert dates
        df["Date_of_report"] = pd.to_datetime(
            df["Date_of_report"], dayfirst=True, errors="coerce"
        )

        if df["Date_of_report"].isnull().all():
            return {"error": "No valid dates found in Date_of_report column"}

        # Find latest month/year
        latest_date = df["Date_of_report"].max()
        latest_month, latest_year = latest_date.month, latest_date.year

        # Filter rows
        df_latest = df[
            (df["Date_of_report"].dt.month == latest_month)
            & (df["Date_of_report"].dt.year == latest_year)
        ]

        total_reports = len(df_latest)
        if total_reports == 0:
            return {"resolution_rate_latest": 0}

        solved_reports = len(df_latest[df_latest["Active"] == "solved"])

        resolution_rate = (solved_reports / total_reports) * 100

        return {
            "total_reports_latest": total_reports,
            "solved_reports_latest": solved_reports,
            "resolution_rate_latest": round(resolution_rate, 2)
        }

    except Exception as e:
        return {"error": str(e)}


@app.get("/peak_hour_analysis")
def peak_hour_analysis():
    try:
        # Fetch data from Supabase
        response = supabase.table("Incident_Reports").select("Report_Time, emergency_type").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"error": "No data available"}

        # Convert Report_Time to datetime (24H format)
        df["Report_Time"] = pd.to_datetime(df["Report_Time"], format="%H:%M:%S", errors="coerce")
        df = df.dropna(subset=["Report_Time"])

        # Extract hour
        df["Hour"] = df["Report_Time"].dt.hour

        # Group by hour and emergency_type
        hourly_counts = df.groupby(["Hour", "emergency_type"]).size().unstack(fill_value=0)

        # Convert to dictionary (hour -> counts per type)
        result = hourly_counts.to_dict(orient="index")

        return {"peak_hour_analysis": result}

    except Exception as e:
        return {"error": str(e)}
    

@app.get("/weekly_patterns")
def weekly_patterns():
    try:
        # Fetch required columns
        response = supabase.table("Incident_Reports").select("Date_of_report, emergency_type").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"weekly_patterns": {}}

        # Convert to datetime
        df["Date_of_report"] = pd.to_datetime(df["Date_of_report"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["Date_of_report"])

        # Extract day of week
        df["Day"] = df["Date_of_report"].dt.day_name()

        # Group by Day + emergency_type
        day_counts = df.groupby(["Day", "emergency_type"]).size().unstack(fill_value=0)

        # Reorder days
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_counts = day_counts.reindex(days_order, fill_value=0)

        # Convert to dictionary
        result = day_counts.to_dict(orient="index")

        return {"weekly_patterns": result}

    except Exception as e:
        return {"error": str(e)}
    

@app.get("/report_types_distribution")
def report_types_distribution():
    try:
        # Fetch Types_of_report column
        response = supabase.table("Incident_Reports").select("Types_of_report").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"report_types_distribution": {}}

        # Count occurrences of each report type
        type_counts = df["Types_of_report"].value_counts().to_dict()

        return {"report_types_distribution": type_counts}

    except Exception as e:
        return {"error": str(e)}

@app.get("/emergency_summary")
def get_emergency_summary():
    try:
        # Fetch relevant columns
        response = (
            supabase.table("Incident_Reports")
            .select("emergency_type, time_taken_to_solve")
            .execute()
        )

        df = pd.DataFrame(response.data)

        if df.empty:
            return []

        # Drop null values
        df = df.dropna(subset=["emergency_type", "time_taken_to_solve"])

        # Group by emergency_type
        summary = (
            df.groupby("emergency_type")
            .agg(
                average_time_to_solve=("time_taken_to_solve", "mean"),
                number_of_reports=("time_taken_to_solve", "count"),
            )
            .reset_index()
        )

        # Round average times
        summary["average_time_to_solve"] = summary["average_time_to_solve"].round(2)

        # Convert to dict
        result = summary.to_dict(orient="records")

        return result

    except Exception as e:
        return {"error": str(e)}