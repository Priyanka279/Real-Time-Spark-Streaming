# simulates a "live" stream by chopping the dataset csv into chunks
# and dropping them into the watch folder one at a time. Spark picks
# them up automatically since it's reading from that directory.
#
# usage:
#   python src/stream_simulator.py --source data/iomt_health_sample.csv --output data/stream_input

import argparse
import csv
import os
import time
import datetime


def chunk_csv(source_path, chunk_size):
    with open(source_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield fieldnames, chunk
                chunk = []
        if chunk:
            yield fieldnames, chunk


def write_chunk(fieldnames, rows, output_dir, batch_num):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"batch_{batch_num:04d}_{ts}.csv"
    path = os.path.join(output_dir, name)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/iomt_health_sample.csv")
    parser.add_argument("--output", default="data/stream_input")
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("--loops", type=int, default=1)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("stream simulator starting")
    print("source:", args.source)
    print("output:", args.output)
    print("chunk size:", args.chunk_size, "interval:", args.interval)

    batch_num = 0
    for loop in range(args.loops):
        for fieldnames, chunk in chunk_csv(args.source, args.chunk_size):
            batch_num += 1
            fname = write_chunk(fieldnames, chunk, args.output, batch_num)
            now = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"[{now}] dropped {fname} ({len(chunk)} rows)")
            time.sleep(args.interval)

    print("done, all batches sent")


if __name__ == "__main__":
    main()
