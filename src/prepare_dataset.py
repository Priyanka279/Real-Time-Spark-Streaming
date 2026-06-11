# one-time script to turn the IoMT excel dataset into a csv that
# looks like a real time-series sensor feed (it doesn't have
# timestamps or repeated patient ids by default)
#
# usage:
#   python src/prepare_dataset.py --input patients_data_with_alerts.xlsx --output data/iomt_health_sample.csv

import argparse
import datetime
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="patients_data_with_alerts.xlsx")
    parser.add_argument("--output", default="data/iomt_health_sample.csv")
    args = parser.parse_args()

    print("reading", args.input)
    df = pd.read_excel(args.input)
    print("loaded", len(df), "rows")

    # there's 50000 unique patient numbers, fold them down to 20
    # repeating patient ids (P001-P020) so each one looks like a
    # patient with lots of readings over time
    df["patient_id"] = "P" + (((df["Patient Number"] - 1) % 20) + 1).astype(str).str.zfill(3)

    # fake timestamps, one reading every 2 seconds starting from a fixed time
    base = datetime.datetime(2024, 1, 15, 8, 0, 0)
    df["timestamp"] = [base + datetime.timedelta(seconds=i * 2) for i in range(len(df))]

    out = df[[
        "timestamp", "patient_id",
        "Heart Rate (bpm)", "SpO2 Level (%)", "Body Temperature (°C)",
        "Systolic Blood Pressure (mmHg)", "Diastolic Blood Pressure (mmHg)",
        "Fall Detection", "Predicted Disease", "Heart Rate Alert",
    ]].copy()

    out.columns = [
        "timestamp", "patient_id",
        "heart_rate", "spo2", "temperature",
        "bp_systolic", "bp_diastolic",
        "fall_detection", "predicted_disease", "heart_rate_alert",
    ]

    out.to_csv(args.output, index=False)
    print("saved", len(out), "rows ->", args.output)
    print("patient ids:", sorted(out["patient_id"].unique()))


if __name__ == "__main__":
    main()
