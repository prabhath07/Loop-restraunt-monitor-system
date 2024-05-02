from fastapi import FastAPI, HTTPException, BackgroundTasks
from sqlalchemy import create_engine, String ,Boolean , DateTime, Integer , Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from multiprocessing import Pool, cpu_count
from pytz import timezone
from dateutil.parser import parse
from datetime import datetime , timedelta
import pandas as pd
import secrets

# Initialize the application
app = FastAPI(title="Loop Restaurant Monitoring System")

# Database configurations pointed out to local db for now
DATABASE_URL = 'postgresql://prabhath_spocto:prabhath@localhost:5433/colending_dev'
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# SQL Alchemy models for storestatus , Store business hours and store timezone

class StoreBusinessHours(Base):
    __tablename__ = 'store_business_hours'
    store_id = Column(String, primary_key=True)
    day_of_week = Column(Integer, primary_key=True)
    start_time_local = Column(String)
    end_time_local = Column(String)

class StoreTimezone(Base):
    __tablename__ = 'store_timezone'
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String, default='America/Chicago')

class StoreStatus(Base):
    __tablename__ = 'store_status'
    store_id = Column(String, primary_key=True)
    timestamp_utc = Column(DateTime, primary_key=True)
    status = Column(Boolean, nullable=False)

class ReportStatus(Base):
    __tablename__ = 'report_status'
    report_id = Column(String, primary_key=True)
    status = Column(Boolean, default=False)

# Creates the database tables
Base.metadata.create_all(bind=engine)

# To generate a unique report id of length 9
def generate_unique_report_id():
    while True:
        report_id = secrets.token_urlsafe(9)
        with SessionLocal() as db:
            existing_report = db.query(ReportStatus).filter_by(report_id=report_id).first()
        if not existing_report:
            return report_id


def convert_to_isoformat(timestamp_str):
    timestamp_dt = parse(timestamp_str)
    isoformat_str = timestamp_dt.astimezone(timezone('UTC')).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return isoformat_str


def convert_local_to_utc(row, df_timezones , column):
    store_id = row['store_id']
    timezone_info = df_timezones.loc[df_timezones['store_id'] == store_id, 'timezone_str']
    if not timezone_info.empty:
        tz_str = timezone_info.values[0]
        tz_local = timezone(tz_str)
        dt_local = datetime.strptime(row[column], '%H:%M:%S')
        dt_local = tz_local.localize(dt_local)
        dt_utc = dt_local.astimezone(timezone('UTC'))
        return dt_utc.strftime('%H:%M:%S')
    else:
        return None

# Processing the poll data , loading databases into pandas dataframe and then sorting according to the store id passed
def preprocess_polls_data(store_id):
    with engine.connect() as conn:
        df_status = pd.read_sql_table('store_status', conn)
        df_business_hours = pd.read_sql_table('store_business_hours', conn)
        df_timezones = pd.read_sql_table('store_timezone', conn)

    # Filter data based on the store ID
    df_status = df_status[df_status['store_id'] == int(store_id)]
    df_business_hours = df_business_hours[df_business_hours['store_id'] == int(store_id)]

    df_status['timestamp_utc'] = pd.to_datetime(df_status['timestamp_utc'].apply(convert_to_isoformat))

    if not df_timezones.empty:
        df_business_hours = df_business_hours[df_business_hours['store_id'].isin(df_timezones['store_id'])]
        df_business_hours['start_time_utc'] = df_business_hours.apply(convert_local_to_utc, args=(df_timezones,'start_time_local'), axis=1)
        df_business_hours['end_time_utc'] = df_business_hours.apply(convert_local_to_utc, args=(df_timezones,'end_time_local'), axis=1)

    return df_status, df_business_hours, df_timezones


def calculate_store_business_hours(df_store_hours):
    current_time = datetime.now()
    current_day = current_time.weekday()
    output_dict = {}
    output_dict['business_hours_in_a_week'] = None
    output_dict['business_hour'] = True
    # return output_dict
    for index, row in df_store_hours.iterrows():
        print(row)
        end_time_utc = datetime.strptime(row['end_time_local'], '%H:%M:%S')
        start_time_utc = datetime.strptime(row['start_time_local'] , '%H:%M:%S')
        if row['day'] == current_day:
            output_dict['business_hours_current_day'] = end_time_utc - start_time_utc
            if start_time_utc.time() < current_time.time() and end_time_utc.time() > current_time.time():
                output_dict['business_hour'] = True
        if output_dict['business_hours_in_a_week'] == None:
            output_dict['business_hours_in_a_week'] = end_time_utc - start_time_utc
        else:
            output_dict['business_hours_in_a_week'] += end_time_utc - start_time_utc

    for key, value in output_dict.items():
        if isinstance(value, timedelta):
            output_dict[key] = value.total_seconds() / (60*60)
        else:
            pass
    return output_dict

# Calculate store statistics
# The logic of this method is explained in logic.txt
def calculate_store_stats(store_id, df_polls, df_store_hours): 
    df_store_polls = df_polls.sort_values(by='timestamp_utc', ascending=False)
    business_hours_data = calculate_store_business_hours(df_store_hours)
    total_uptime_minutes_hour = 0
    total_uptime_minutes_day = 0
    total_uptime_minutes_week = 0
    active_start_time = None
    consider_hour_uptime = True
    consider_day_uptime = True
    consider_week_uptime = True
    # Iterate through each observation
    for index, row in df_store_polls.iterrows():
        if row['status'] == 'active':
            if active_start_time is not None:
                if business_hours_data['business_hour'] == True and consider_hour_uptime:
                    if ((datetime.now() - row['timestamp_utc'].tz_localize(None)).total_seconds() < 60*60):
                        total_uptime_minutes_hour += (active_start_time - row['timestamp_utc'].tz_localize(None)).total_seconds() / 60 
                    elif consider_hour_uptime and (datetime.now() - active_start_time).total_seconds() < 60*60 and (row['timestamp_utc'].tz_localize(None) - active_start_time).total_seconds() < 60*60 :
                        consider_hour_uptime = False
                        total_uptime_minutes_hour = max(60 , (total_uptime_minutes_hour + (active_start_time - row['timestamp_utc'].tz_localize(None))).total_seconds()/60)
                    else:
                        consider_hour_uptime = False
                if ((datetime.now() - row['timestamp_utc'].tz_localize(None)).total_seconds() < 60*60*24) and consider_day_uptime:
                        total_uptime_minutes_day += (active_start_time - row['timestamp_utc'].tz_localize(None)).total_seconds() / 60 
                elif consider_day_uptime and (datetime.now() - active_start_time).total_seconds() < 60*60*24 and (row['timestamp_utc'].tz_localize(None) - active_start_time).total_seconds() < 60*60*24 :
                    consider_day_uptime = False
                    total_uptime_minutes_day = max(60*24 , (total_uptime_minutes_day + (active_start_time - row['timestamp_utc'].tz_localize(None))).total_seconds()/60)
                else:
                    consider_day_uptime = False
                if ((datetime.now() - row['timestamp_utc'].tz_localize(None)).total_seconds() < 60*60*24*7) and consider_week_uptime:
                        total_uptime_minutes_week += (active_start_time - row['timestamp_utc'].tz_localize(None)).total_seconds() / 60 
                elif consider_week_uptime and (datetime.now() - active_start_time).total_seconds() < 60*60*24*7 and (row['timestamp_utc'].tz_localize(None) - active_start_time).total_seconds() < 60*60*24*7 :
                    consider_week_uptime = False
                    total_uptime_minutes_week = max(60*24*7 , (total_uptime_minutes_week + (active_start_time - row['timestamp_utc'].tz_localize(None))).total_seconds()/60)
                else:
                    consider_week_uptime = False
                    break
            active_start_time = row['timestamp_utc'].tz_localize(None)
        else:
            # Reset active start time for the next active period
            active_start_time = None

    result = {
        'store id': store_id,
        'uptime last hour':"{} minutes".format(total_uptime_minutes_hour),
        'uptime last day': "{} hours".format(total_uptime_minutes_day/ 60),
        'uptime last week': "{} hours".format(total_uptime_minutes_hour/ (60)),
        'downtime last hour': "{} minutes".format(60 - total_uptime_minutes_hour),
        'downtime last day': "{} hours".format((60*24 - total_uptime_minutes_hour)/ 60),
        'downtime last week': "{} hours".format((60*24 - total_uptime_minutes_hour)/ (24)),
        'total business hours in a week ': "{} hours".format(business_hours_data['business_hours_in_a_week']),
        'total business hours today': "{} hours".format(business_hours_data['business_hours_current_day'])
    }
    return result

# Compute and save report
def compute_and_save_report(report_id , store_id):
    df_polls, df_business_hours, df_timezones = preprocess_polls_data(store_id)
    num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        results = pool.starmap(calculate_store_stats, [(store_id, df_polls, df_business_hours)])
    report_df = pd.DataFrame(results)

    if not os.path.exists('Report Storage'):
        os.makedirs('Report Storage')

    report_file_path = f"Report Storage/report_{report_id}.csv"
    report_df.to_csv(report_file_path, index=False)

    with SessionLocal() as db:
        existing_report_status = db.query(ReportStatus).filter_by(report_id=report_id).first()

        if existing_report_status:
            existing_report_status.status = True
            db.commit()
        else:
            report_status = ReportStatus(report_id=report_id, status=True)
            db.add(report_status)
            db.commit()

# API to trigger report generation
@app.post("/trigger_report/{store_id}", tags=["Reports"], response_model=dict)
async def trigger_report(background_tasks: BackgroundTasks , store_id: str):
    report_id = generate_unique_report_id()
    with SessionLocal() as db:
        report_status = ReportStatus(report_id=report_id, status=False)
        db.add(report_status)
        db.commit()
    background_tasks.add_task(compute_and_save_report, report_id , store_id)

    return {"report_id": report_id}

# API to get report
@app.get("/get_report/{report_id}/", tags=["Reports"], response_model=dict)
async def get_report(report_id: str):
    with SessionLocal() as db:
        report_status = db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()
    if not os.path.exists('Report Storage'):
        os.makedirs('Report Storage')
    report_file_path = f"Report Storage/report_{report_id}.csv"

    if os.path.exists(report_file_path):
        if report_status is None:
            with SessionLocal() as db:
                report_status = ReportStatus(report_id=report_id, status=True)
                db.add(report_status)
                db.commit()
            return {"status": "Complete", "csv_file": report_file_path}
        else:
            return {"status": "Complete", "csv_file": report_file_path}
    else:
        if report_status is not None:
            with SessionLocal() as db:
                report_status.status = False
                db.commit()
        else:
            raise HTTPException(status_code=404, detail="Report not found")

        return {"status": "Running"}