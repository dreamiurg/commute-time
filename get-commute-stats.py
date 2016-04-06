import arrow
import csv
import boto3
import datetime
import logging
import numpy as np
import os
import re
import sys
import StringIO
import requests
from argparse import ArgumentParser

from const import *


def get_opts():
    """
    Return the cli opts

    returns ArgumentParser
    """
    p = ArgumentParser(
        description='This script will parse log file and print commute time stats from FROM location to TO location.')
    p.add_argument(
        '-f', '--file', help='Path to csv file with location enter/exit times')
    p.add_argument(
        '-u', '--url', help='Url for csv file with location enter/exit times [default: %(default)s]', default=URL)
    p.add_argument('--from', dest="from_loc",
                   help="Name of the FROM location [default: %(default)s]", default="Home")
    p.add_argument('--to', dest="to_loc",
                   help="Name of the TO location [default: %(default)s]", default="Work")
    p.add_argument('-D', '--debug', dest='debug', action='store_true',
                   default=False, help='Turn on debug logging [default: %(default)s]')
    opts = p.parse_args()

    return opts


def get_logger(opts):
    """
    Gets a logger object

    returns logging.Logger
    """
    logger = logging.getLogger()
    level = logging.DEBUG if opts.debug else logging.WARN
    logger.setLevel(level)

    ch = logging.StreamHandler(sys.stdout)
    #formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger


def is_exited_location(expected_location, action, location):
    return (action == "exited" and location == expected_location)


def is_entered_location(expected_location, action, location):
    return (action == "entered" and location == expected_location)


def get_csv_reader_from_file(filename):
    csvfile = open(filename, 'rb')
    return csv.reader(csvfile)


def get_csv_reader_from_url(url):
    r = requests.get(url)
    csvstream = StringIO.StringIO(r.text)
    return csv.reader(csvstream)


def get_commute_times(opts, logger, csvreader):
    commute_times = []

    exited_time = None

    for row in csvreader:
        action = row[0]
        # Need to replace "March 09, 2016 at 09:23AM" -> "March 09, 2016 09:23AM" because arrow.get() does not allow
        # escaping chars in the format string, making it impossible to parse " at " in the datetime string, see
        # https://github.com/crsmithdev/arrow/issues/35
        time = arrow.get(re.sub(' at ', ' ', row[1]), "MMMM DD, YYYY HH:mmA")
        location = row[2]

        if is_exited_location(opts.from_loc, action, location) and exited_time is None:
            exited_time = time
            logger.debug("Exited {} at {}".format(location, time))
            continue

        if exited_time is not None and is_entered_location(opts.to_loc, action, location):
            entered_time = time
            commute_time = (entered_time - exited_time)
            commute_times.append(commute_time.seconds)
            logger.debug("Entered {} at {} right after exiting {}, commute took {}".format(
                location, time, opts.from_loc, commute_time))

        # if nothing else applies, reset timer
        exited_time = None

    return commute_times


def get_commute_stats(opts, logger, csvreader, percentiles=[50, 95, 100]):
    commute_times = get_commute_times(opts, logger, csvreader)
    np_commute_times = np.array(commute_times)
    stats = map(lambda x: str(datetime.timedelta(seconds=x)),
                np.percentile(np_commute_times, percentiles))
    return stats, commute_times


def lambda_handler(event, context):
    opts = type('obj', (object,), {
        'from_loc': 'Home',
        'to_loc': 'Work',
        'debug': False
    })
    logger = get_logger(opts)

    csvreader = get_csv_reader_from_url(URL)
    stats, commute_times = get_commute_stats(opts, logger, csvreader)
    msg = "Your travel time from {} to {} is {} @ p50, {} @ p95 and {} @ p100 based on {} measurements.".format(
        opts.from_loc, opts.to_loc, stats[0], stats[1], stats[2], len(commute_times))

    client = boto3.client('sns')
    response = client.publish(TopicArn = TOPIC_ARN, Message = msg)

    return response


def main():
    opts = get_opts()
    logger = get_logger(opts)

    try:
        csvreader = None
        if opts.file:
            csvreader = get_csv_reader_from_file(opts.file)
        else:
            csvreader = get_csv_reader_from_url(opts.url)

        stats, commute_times = get_commute_stats(opts, logger, csvreader)
        print "Your travel time from {} to {} is {} @ p50, {} @ p95 and {} @ p100 based on {} measurements.".format(
            opts.from_loc, opts.to_loc, stats[0], stats[1], stats[2], len(commute_times))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error('Unknown error has occurred, exiting: {0}'.format(e))
        if opts.debug:
            import traceback
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    main()
