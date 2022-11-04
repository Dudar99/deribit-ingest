import argparse
from ingester.ingest import DeribitIngester


def get_args():
    parser = argparse.ArgumentParser(description='Arguments to manage run')

    parser.add_argument('-s', '--start_timestamp',
                        action="store", dest="start",
                        help="Start timestamp", required=False)

    parser.add_argument('-e', '--end_timestamp',
                        action="store", dest="end",
                        help="End timestamp", required=False)

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()

    ingester = DeribitIngester(start_timestamp=args.start, end_timestamp=args.end)
    ingester.ingest()
