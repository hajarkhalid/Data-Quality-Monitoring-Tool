import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
import json
import logging
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import schedule
import time
import os
from retrying import retry

# Setup logging
logging.basicConfig(
    filename=f"data_quality_log_{datetime.now().strftime('%Y%m%d')}.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load configuration
with open("data_quality_config.json", "r") as f:
    CONFIG = json.load(f)

# Retry decorator for email sending
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def send_email_alert(subject, body):
    sender = CONFIG["email"]["sender"]
    receiver = CONFIG["email"]["receiver"]
    password = CONFIG["email"]["password"]

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        logging.info("Email alert sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        raise

class DataQualityMonitor:
    def __init__(self):
        self.engine = create_engine(CONFIG["database"]["connection_string"])
        self.df = None
        self.report = []
        self.load_data()

    def load_data(self):
        """Load data from database using SQL query."""
        try:
            self.df = pd.read_sql(CONFIG["database"]["query"], self.engine)
            logging.info(f"Data loaded successfully: {self.df.shape}")
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            self.report.append(f"Data Load Error: {e}")

    def check_missing_values(self):
        """Check for missing values based on config thresholds."""
        missing = self.df.isnull().sum()
        total_missing = missing.sum()
        if total_missing > CONFIG["thresholds"]["missing_values"]:
            self.report.append(f"Missing Values Exceed Threshold: {total_missing}")
            logging.warning(f"Missing values: {missing[missing > 0].to_dict()}")
        return missing[missing > 0]

    def check_duplicates(self):
        """Check for duplicate rows."""
        duplicates = self.df.duplicated().sum()
        if duplicates > CONFIG["thresholds"]["duplicates"]:
            self.report.append(f"Duplicates Exceed Threshold: {duplicates}")
            logging.warning(f"Found {duplicates} duplicate rows")
        return duplicates

    def check_anomalies(self):
        """Detect anomalies using Isolation Forest."""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if not numeric_cols.empty:
            iso_forest = IsolationForest(contamination=CONFIG["anomaly"]["contamination"], random_state=42)
            anomalies = iso_forest.fit_predict(self.df[numeric_cols].fillna(0))
            anomaly_count = (anomalies == -1).sum()
            if anomaly_count > 0:
                self.report.append(f"Anomalies Detected: {anomaly_count} rows")
                logging.warning(f"Anomalies in rows: {self.df[anomalies == -1].index.tolist()}")
            return self.df[anomalies == -1]
        return pd.DataFrame()

    def custom_checks(self):
        """Run custom rules from config."""
        for rule in CONFIG["custom_rules"]:
            column = rule["column"]
            condition = rule["condition"]
            threshold = rule["threshold"]
            try:
                if condition == "max":
                    violations = self.df[self.df[column] > threshold]
                elif condition == "min":
                    violations = self.df[self.df[column] < threshold]
                if not violations.empty:
                    self.report.append(f"Custom Rule Violation ({column} {condition} {threshold}): {len(violations)} rows")
                    logging.warning(f"Custom rule violation: {violations.head().to_dict()}")
            except Exception as e:
                logging.error(f"Error in custom check {rule}: {e}")

    def generate_report(self):
        """Generate and send report if issues are found."""
        if self.report:
            report_str = "\n".join(self.report)
            subject = f"Data Quality Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            logging.info("Generating report with issues.")
            send_email_alert(subject, report_str)
        else:
            logging.info("No data quality issues detected.")

    def run_checks(self):
        """Run all data quality checks."""
        self.check_missing_values()
        self.check_duplicates()
        self.check_anomalies()
        self.custom_checks()
        self.generate_report()

# Scheduling function
def job():
    monitor = DataQualityMonitor()
    monitor.run_checks()

if __name__ == "__main__":
    # Schedule to run every hour (configurable)
    schedule.every(CONFIG["schedule"]["interval_minutes"]).minutes.do(job)
    
    # Initial run
    job()

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)