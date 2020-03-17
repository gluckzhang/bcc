#!/usr/bin/python
# -*- coding: utf-8 -*-
# Filename: realistic_failures.py

import csv, requests, sys, getopt, datetime, time
from prettytable import PrettyTable
import logging

PROMETHEUS_URL = ''
QUERY_API = '/api/v1/query'
RANGE_QUERY_API = '/api/v1/query_range'
OUTPUTFILE = '' # default: result.csv
START = '' # rfc3339 | unix_timestamp
END = '' # rfc3339 | unix_timestamp
STEP = ''
PERIOD = 60 # unit: miniute, default 60

def main():
    handle_args(sys.argv[1:])

    failure_category = query_failure_category(START, END, STEP)
    failure_details = query_failure_detail(failure_category, START, END, STEP)
    failure_details = calculate_failure_rate(failure_details, STEP)

    pretty_print_details(failure_details)

def handle_args(argv):
    global PROMETHEUS_URL
    global OUTPUTFILE
    global START
    global END
    global STEP
    global PERIOD

    try:
        opts, args = getopt.getopt(argv, "h:o:c:s:", ["host=", "outfile=", "step=", "help", "start=", "end=", "period="])
    except getopt.GetoptError as error:
        logging.error(error)
        print_help_info()
        sys.exit(2)

    for opt, arg in opts:
        if opt == "--help":
            print_help_info()
            sys.exit()
        elif opt in ("-h", "--host"):
            PROMETHEUS_URL = arg
        elif opt in ("-o", "--outfile"):
            OUTPUTFILE = arg
        elif opt in ("-s", "--step"):
            STEP = arg
        elif opt == "--start":
            START = arg
        elif opt == "--end":
            END = arg
        elif opt == "--period":
            PERIOD = int(arg)

    if PROMETHEUS_URL == '':
        logging.error("You should use -h or --host to specify your prometheus server's url, e.g. http://prometheus:9090")
        print_help_info()
        sys.exit(2)

    if OUTPUTFILE == '':
        OUTPUTFILE = 'result.csv'
        logging.warning("You didn't specify output file's name, will use default name %s", OUTPUTFILE)
    if STEP == '':
        STEP = '15s'
        logging.warning("You didn't specify query resolution step width, will use default value %s", STEP)
    if PERIOD == '' and START == '' and END == '':
        PERIOD = 10
        logging.warning("You didn't specify query period or start&end time, will query the latest %s miniutes' data as a test", PERIOD)

def print_help_info():
    print('')
    print('realistic_failures Help Info')
    print('    realistic_failures.py -h <prometheus_url> [-o <outputfile>]')
    print('or: realistic_failures.py --host=<prometheus_url> [--outfile=<outputfile>]')
    print('---')
    print('Additional options: --start=<start_timestamp_or_rfc3339> --end=<end_timestamp_or_rfc3339> --period=<get_for_most_recent_period(int miniutes)>')
    print('                    use start&end or only use period')

def query_failure_category(start_time, end_time, step):
    failure_category = list()

    query_string = 'increase(failed_syscalls_total{error_code!="SUCCESS"}[%s])'%step
    response = requests.post(PROMETHEUS_URL + RANGE_QUERY_API, data={'query': query_string, 'start': start_time, 'end': end_time, 'step': step})
    status = response.json()["status"]

    if status == "error":
        logging.error(response.json())
        sys.exit(2)
    
    results = response.json()['data']['result']

    for entry in results:
        failure_category.append({
            "syscall_name": entry["metric"]["syscall_name"],
            "error_code": entry["metric"]["error_code"]
        })
    
    return failure_category

def query_failure_detail(failure_category, start_time, end_time, step):
    failure_details = list()

    query_string = 'increase(failed_syscalls_total{error_code!="SUCCESS",syscall_name="%s"}[%s])>0'
    for category in failure_category:
        response = requests.post(PROMETHEUS_URL + RANGE_QUERY_API, data={'query': query_string%(category["syscall_name"], step), 'start': start_time, 'end': end_time, 'step': step})
        status = response.json()["status"]

        if status == "error":
            logging.error(response.json())
            sys.exit(2)
        
        results = filter(lambda x: len(x) > 0, response.json()['data']['result'])

        for entry in results:
            failure_details.append({
                "syscall_name": entry["metric"]["syscall_name"],
                "error_code": entry["metric"]["error_code"],
                "cases_in_total": len(entry["values"]),
                "samples": [
                    {"timestamp": entry["values"][0][0], "failures_count": entry["values"][0][1]}, # first case
                    {"timestamp": entry["values"][-1][0], "failures_count": entry["values"][-1][1]} # last case
                ]
            })

    return failure_details

def calculate_failure_rate(failure_details, step):
    query_string = 'sum without(error_code)(increase(failed_syscalls_total{syscall_name="%s"}[%s]))'

    for detail in failure_details:
        for sample in detail["samples"]:
            response = requests.post(PROMETHEUS_URL + QUERY_API, data={'query': query_string%(detail["syscall_name"], step), 'time': sample["timestamp"], 'step': step})
            status = response.json()["status"]

            if status == "error":
                logging.error(response.json())
                continue
        
            sample["total_count"] = float(response.json()["data"]["result"][0]["value"][1])
            sample["failure_rate"] = float(sample["failures_count"]) / sample["total_count"]

    return failure_details

def pretty_print_details(failure_details):
    stat_table = PrettyTable()
    stat_table.field_names = ["Syscall Name", "Error Code", "Cases in Total", "Samples"]

    for detail in failure_details:
        samples_str = ""
        for sample in detail["samples"]:
            localtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sample["timestamp"]))
            samples_str += "localtime: %s, failure rate: %2f\n"%(localtime, sample["failure_rate"])
        samples_str = samples_str[:-1]
        stat_table.add_row([detail["syscall_name"], detail["error_code"], detail["cases_in_total"], samples_str])

    print(stat_table)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()