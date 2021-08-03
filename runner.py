import argparse
import datetime
import logging
import os
import sys

import pandas as pd
import requests
import time
import yaml
import zipfile

from discord_webhook import DiscordWebhook
from pathlib import Path

from utils import (
    SQLiteExecutor,
    logging_map,
    create_log_dir,
    flatten_list,
    paths
)

FINANCIAL_DISCLOSURE_HOME = "https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure"
FINANCIAL_DISCLOSURE_REPORT_URL = 'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP'
PERIODIC_TRANSACTION_REPORT_URL = 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf'

def post_message_discord(config, message):
    logger.info(message)
    webhook = DiscordWebhook(
        url=config['url'],
        username="RepresentativesAlert",
        content=message
    )
    webhook.execute()


def doc_id_exists(doc_id, doc_ids):
    if doc_id in doc_ids:
        return True
    return False


if __name__ == "__main__":
    # Setup argument parser
    parser = argparse.ArgumentParser('Gather configs for fetching ticker data')
    parser.add_argument('-log-level', '--log-level',
                        help='Set the level for logging',
                        choices=('DEBUG', 'INFO', 'WARNING', 'ERROR'),
                        default='INFO')

    parser.add_argument("-e", "--env",
                        help="Environement the code is running on",
                        type=str, default='local')

    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging_map[args.log_level])

    log_directory, log_file_name = create_log_dir('HORScrapper', args.env)
    log_format = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s",datefmt="%H:%M:%S")

    file_handler = logging.FileHandler(log_directory + "/" + log_file_name)
    file_handler.setLevel(logging_map[args.log_level])
    file_handler.setFormatter(log_format)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    day = datetime.datetime.now()
    year = day.strftime("%Y")

    config_path = '{}/configs/config.yml'.format(
        paths[args.env]
    )
    with open(config_path) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # DOWNLOAD YEALRY ZIP FILE
    r = requests.get(FINANCIAL_DISCLOSURE_REPORT_URL.format(
        year=year
    ))

    # Save zip file
    zipfilename = '{}/data/fdrs_zip/{}.zip'.format(
        paths[args.env], year
    )
    with open(zipfilename, 'wb') as f:
        f.write(r.content)

    # Extract zip file
    fdrs_dir = "{}/data/fdrs".format(paths[args.env])
    with zipfile.ZipFile(zipfilename) as z:
        z.extractall(fdrs_dir)

    # Open content of fdr file

    filename = '{}/data/fdrs/{}FD.txt'.format(
        paths[args.env], year
    )
    fdrs = pd.read_csv(filename, sep='\t')

    # SQLiteExecutor object
    sql_executor = SQLiteExecutor(
        env=args.env, logger=logger,
        path_to_db='{}data/sql/fdr.db'.format(
            paths[args.env]
        )
    )
    # Get DocID from DB
    doc_ids = flatten_list(
        sql_executor.execute_query("select DocID from fdr")
    )

    # Only keep new DocIDs
    fdrs['NewID'] = fdrs.apply(lambda x: doc_id_exists(x.DocID, doc_ids), axis=1)
    new_fdrs = fdrs[fdrs['NewID'] == False].drop(['NewID'], axis = 1)

    if new_fdrs.empty:
        message = "No new trades from representatives"
        post_message_discord(config['discord'], message)
    else:
        message = "New trades from representatives!\n{home_url}".format(
            home_url=FINANCIAL_DISCLOSURE_HOME
        )
        post_message_discord(config['discord'], message)

        for idx, row in new_fdrs.iterrows():
            name = "{first} {last}".format(
                first=row['First'],
                last=row['Last']
            )
            url = PERIODIC_TRANSACTION_REPORT_URL.format(
                year=year, doc_id=row['DocID']
            )
            message = "{name} traded! {docid}: {url}".format(
                name=name,
                docid=row['DocID'],
                url=url
                )
            post_message_discord(config['discord'], message)
            time.sleep(3)

        # Save content of file in sqlite fdr table
        sql_executor.save_pandas_df(new_fdrs, table='fdr')


    # Delete content from dirs
    path = Path("{}/data/".format(paths[args.env]))
    for dir in path.iterdir():
        if dir.__str__().split('/')[-1] not in  ["sql", ".DS_Store"]:
            logger.info("Dir: {}".format(dir))
            sub_path = Path(dir.__str__())
            for file in sub_path.iterdir():
                logger.info("Removing {}".format(file))
                os.remove(file)
