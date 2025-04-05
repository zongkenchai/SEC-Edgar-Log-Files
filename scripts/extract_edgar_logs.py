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
                        action='store',
                        required=True)
    
    args = args_parser.parse_args()
    start_date = pendulum.parse(args.start_date, strict=False)
    end_date = pendulum.parse(args.end_date, strict=False)



    for date in (end_date- start_date).range('days'):
        print(f"Processing {date.to_date_string()}")
        SECEdgar().preprocess(date=date.to_date_string())
