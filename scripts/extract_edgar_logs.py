from tracemalloc import start
import pendulum
from argparse import ArgumentParser
from lib.sec_edgar import SECEdgar 
from lib.logging_config import get_logger


logger = get_logger(__name__)

if __name__ == "__main__":
    args_parser = ArgumentParser(
        description='Extract SEC Edgar logs from 2003-2017')
    
    args_parser.add_argument("--start-date",
                            help="Start date for the logs",
                            dest='start_date',
                            action='store',
                            required=True)
    
    args_parser.add_argument("--end-date",
                        help="End date for the logs",
                        dest='end_date',
                        action='store')
    
    args_parser.add_argument("--force",
                        help="Force reprocessing of existing files",
                        dest='force',
                        action='store_true',
                        default=False)
    
    args = args_parser.parse_args()
    start_date = pendulum.parse(args.start_date, strict=False)
    if args.end_date:
        end_date = pendulum.parse(args.end_date, strict=False)
    else:
        end_date = start_date

    for date in (end_date- start_date).range('days'):
        print(f"Processing {date.to_date_string()}")
        SECEdgar(force=args.force).preprocess(date=date.to_date_string())
