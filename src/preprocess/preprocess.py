import re
import sys
import os
import json
import argparse
import logging
import pyorc
import datetime
import user_agents
import psycopg2
from psycopg2.extras import execute_values


empty_entry = {
    'artist': None,
    'sessionId': None,
    'userId': None,
    'location': None,
    'status': None,
    'userAgent': None,
    'userAgent_browser_family': None,
    'userAgent_browser_version': None,
    'userAgent_os_family': None,
    'userAgent_os_version': None,
    'userAgent_is_mobile': None,
    'userAgent_is_pc': None,
    'userAgent_is_tablet': None,
    'lastName': None,
    'method': None,
    'registration': None,
    'registration_timestamp': None,
    'song': None,
    'level': None,
    'itemInSession': None,
    'ts': None,
    'timestamp': None,
    'firstName': None,
    'length': None,
    'auth': None,
    'gender': None,
    'page': None
}


class Preprocessor:
    """
    This is the main class that holds all logic for preprocessing the json files and stores them as ORC files
    """

    schema = pyorc.Struct(
        artist=pyorc.String(),
        sessionId=pyorc.Int(),
        userId=pyorc.String(),
        location=pyorc.String(),
        status=pyorc.Int(),
        userAgent=pyorc.String(),
        userAgent_browser_family=pyorc.String(),
        userAgent_browser_version=pyorc.String(),
        userAgent_os_family=pyorc.String(),
        userAgent_os_version=pyorc.String(),
        userAgent_is_mobile=pyorc.Boolean(),
        userAgent_is_pc=pyorc.Boolean(),
        userAgent_is_tablet=pyorc.Boolean(),
        lastName=pyorc.String(),
        method=pyorc.String(),
        registration=pyorc.Double(),
        registration_timestamp=pyorc.Timestamp(),
        song=pyorc.String(),
        level=pyorc.String(),
        itemInSession=pyorc.Int(),
        ts=pyorc.Double(),
        timestamp=pyorc.Timestamp(),
        firstName=pyorc.String(),
        length=pyorc.Float(),
        auth=pyorc.String(),
        gender=pyorc.String(),
        page=pyorc.String()
    )

    def __init__(self):
        """
        Constructor for our Data Preparation class
        """
        # setting up input arguments
        parser = self._setup_arguments()
        self.args = parser.parse_args()

        # configure logging
        loglevel = logging.INFO
        if self.args == 'WARNING':
            loglevel = logging.WARNING
        elif self.args == 'DEBUG':
            loglevel = logging.DEBUG
        elif self.args == 'WARNING':
            loglevel = logging.WARNING
        elif self.args == 'ERROR':
            loglevel = logging.ERROR,
        elif self.args == 'FATAL':
            loglevel = logging.FATAL

        logging.basicConfig(level=loglevel, format='%(asctime)s - %(levelname)s %(message)s')

        self.database_connection: psycopg2.extensions.connection = None
        self.database_cursor: psycopg2.extensions.cursor = None

        self.insert_query = '''
            INSERT INTO public.sparkify_logs
                (artist, sessionid, userid, "location", status, useragent, useragent_browser_family, 
                useragent_browser_version, useragent_os_family, useragent_os_version, useragent_is_mobile, 
                useragent_is_pc, useragent_is_tablet, lastname, "method", registration, registration_timestamp, 
                song, "level", iteminsession, ts, "timestamp", firstname, length, auth, gender, page)
            VALUES %s;

        '''
        self.database_insert_data = []

        self.writers = {}

    def _setup_arguments(self) -> argparse.ArgumentParser:
        """
        Setup our input arguments
        :return:
        """
        parser = argparse.ArgumentParser(usage='python3 preprocess.py <arguments>',
                                         description='Preprocess JSON files containing SPARKIFY API logs',
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument('--file',
                            '-f',
                            help='The JSON file containing the API logs from SPARKIFY',
                            type=str,
                            dest='file',
                            default=os.path.join('..', '..', 'data', 'source', 'mini_sparkify_event_data.json'))

        parser.add_argument('--loglevel',
                            '-l',
                            help='Loglevel for printing console outputs',
                            type=str,
                            choices=['INFO', 'DEBUG', 'WARNING', 'ERROR', 'FATAL'],
                            default='INFO',
                            dest='loglevel')

        parser.add_argument('--outputfolder',
                            '-o',
                            help='The target folder where to store the orc data',
                            type=str,
                            default=os.path.join('..', '..', 'data', 'orc'),
                            dest='outputfolder')

        parser.add_argument('--stripesize',
                            '-s',
                            help='The stripe size to use for the ORC output files',
                            type=int,
                            default=10240,
                            dest='stripesize')

        parser.add_argument('--database_export',
                            help='Specifies if we want to also export our entries to the database for '
                                 'visualization, exploration purposes',
                            action='store_true',
                            default=True,
                            dest='database_export')

        parser.add_argument('--database_host',
                            help='The host of database where we want to store the entries',
                            type=str,
                            default='localhost',
                            dest='database_host')

        parser.add_argument('--database_port',
                            help='The port of the database where we want to store the entries',
                            type=int,
                            default=5432,
                            dest='database_port')

        parser.add_argument('--database_name',
                            help='The name of the database to use where we want to store our entries',
                            type=str,
                            default='postgres',
                            dest='database_name')

        parser.add_argument('--database_user',
                            help='the username for the connection to the target database',
                            type=str,
                            default='postgres',
                            dest='database_user')

        parser.add_argument('--database_password',
                            help='the password for the connection to the target database',
                            type=str,
                            default='postgres',
                            dest='database_password')

        return parser

    def run(self) -> int:
        """
        Run the Data Preparation
        :return:
        """
        result = 0
        writer = None
        try:
            logging.info('Start processing data')

            self.check_source_file()
            self.check_and_create_target_dir()

            # Opening our database connection if required
            if self.args.database_export:
                self.database_connect()

            logging.info('Start reading file')
            with open(self.args.file) as file:
                for line in file:
                    try:
                        j = self.process_entry(line)
                        date = j['timestamp'].strftime("%Y-%m-%d")

                        # Write log entry to orc files
                        if date not in self.writers.keys():
                            self.create_writer(date)
                        self.writers[date]['writer'].write(j)

                        # Write log entry to database
                        if self.args.database_export:
                            self.database_insert_data.append(tuple(j.values()))

                            if len(self.database_insert_data) >= 5000:
                                logging.info('Writing database chunk')
                                execute_values(self.database_cursor, self.insert_query, self.database_insert_data)
                                self.database_insert_data = []

                    except Exception as error:
                        logging.warning('Could not process log line: %s', str(error.args))

        except Exception as error:
            logging.fatal('Problem during preprocessing data: %s', str(error.args))
            result = 1

        # closing all writers
        self.close_writers()

        # closing our database connection if it exists
        self.database_close()

        return result

    def check_source_file(self):
        """
        Check if the provided source file exists
        :return: None
        :raises: FilerNotFoundError
        """
        logging.info('Check if source file exists')
        if not os.path.exists(self.args.file):
            logging.fatal('Source file does not exist')
            raise FileNotFoundError('Input file not found')

    def check_and_create_target_dir(self):
        """
        Check if the target directory exists. If it does not exist, we want to create it
        :return: None
        :raises: OSError
        """
        logging.debug('Check target directory')
        # set up target directory
        if not (os.path.exists(self.args.outputfolder)):
            logging.info('Creating target directory')
            os.makedirs(self.args.outputfolder)

    def process_entry(self, logentry: dict) -> dict:
        """
        This method takes one log entry, ensures that all dict fields are available and enriches the entry with
        further metadata
        :param logentry: The logentry that was read from the json file
        :return: Updated dictionary with further metadata and all necessary fields available
        """
        j = empty_entry.copy()
        j.update(json.loads(logentry))

        if j['userAgent']:
            agent_info = user_agents.parse(j['userAgent'])
            j['userAgent_browser_family'] = agent_info.browser.family
            j['userAgent_browser_version'] = agent_info.browser.version_string
            j['userAgent_os_family'] = agent_info.os.family
            j['userAgent_os_version'] = agent_info.os.version_string
            j['userAgent_is_mobile'] = agent_info.is_mobile
            j['userAgent_is_pc'] = agent_info.is_pc
            j['userAgent_is_tablet'] = agent_info.is_tablet
        else:
            logging.debug('No user agent information provided')

        if j['ts'] is not None:
            j['timestamp'] = datetime.datetime.utcfromtimestamp(j['ts'] / 1000)  # need to convert the ts into seconds
        if j['registration'] is not None:
            j['registration_timestamp'] = datetime.datetime.utcfromtimestamp(j['registration'] / 1000)
        return j

    def create_writer(self, datestring: str):
        """
        We are using this function to create an ORC writer for each log entry date. It will add the writer
        to our internal structure of writers. We need to keep info of all writer so we can reuse them and close
        all of them after we are finished
        :return: None
        """
        date_folder = 'date=' + datestring
        logging.info('Creating new writer for %s', datestring)
        writer_folder = os.path.join(self.args.outputfolder, date_folder)

        if not os.path.exists(writer_folder):
            os.makedirs(writer_folder)

        output = open(os.path.join(writer_folder, 'data.orc'), 'wb')
        writer = pyorc.Writer(output,
                              stripe_size=self.args.stripesize,
                              batch_size=1024,
                              schema=self.schema,
                              struct_repr=pyorc.StructRepr.DICT)
        self.writers[datestring] = {
            'output': output,
            'writer': writer
        }

    def close_writers(self):
        """
        Helper function to close all opened orc writers
        :return: None
        """
        for key, item in self.writers.items():
            if 'writer' in item.keys():
                try:
                    logging.info('Closing ORC stream for %s', key)
                    item['writer'].close()
                except Exception as error:
                    logging.error('Could not close ORC stream for %s', key)
                    logging.error('Message: %s', str(error.args))

    def database_connect(self):
        """
        This method is used for checking if the database connection can be established
        :return: None
        :raises: psycopg2Error
        """
        logging.info('Connecting to target database')
        self.database_connection = connection = psycopg2.connect(
            host=self.args.database_host,
            port=self.args.database_port,
            dbname=self.args.database_name,
            user=self.args.database_user,
            password=self.args.database_password)
        self.database_connection.autocommit = True

        self.database_cursor = self.database_connection.cursor()

        # now we need to ensure that our tables are all ready and set up if they do not exist
        self.database_cursor.execute("select exists(select * from information_schema.tables where table_name=%s)",
                                     ('sparkify_logs',))
        table_exists = self.database_cursor.fetchone()[0]

        if not table_exists:
            self.create_database_table()

    def database_close(self):
        """
        Used to close our database connection
        :return: None
        """
        if self.args.database_export and self.database_connection is not None:
            logging.info('Closing database connection')
            self.database_connection.close()

    def create_database_table(self):
        """
        This method will create our database table
        :return:
        """
        logging.info('Creating database target table')
        query = '''
            create table if not exists sparkify_logs (
            id serial,
            artist text,
            sessionId decimal,
            userId varchar(50),
            location text,
            status decimal,
            userAgent varchar(150),
            userAgent_browser_family varchar(50),
            userAgent_browser_version varchar(15),
            userAgent_os_family varchar(50),
            userAgent_os_version varchar(15),
            userAgent_is_mobile varchar(50),
            userAgent_is_pc varchar(50),
            userAgent_is_tablet varchar(50),
            lastName varchar(50),
            method varchar(10),
            registration decimal,
            registration_timestamp timestamptz,
            song text,
            level varchar(10),
            itemInSession decimal,
            ts decimal,
            timestamp timestamptz not null,
            firstName varchar(50),
            length float,
            auth varchar(50),
            gender varchar(1),
            page text
        ) partition by range(timestamp);
         
        create index sparkify_logs_timestamp_btree on sparkify_logs using btree (timestamp);
        create index sparkify_logs_timestamp_brin on sparkify_logs using brin (timestamp);
        create index sparkify_logs_artist on sparkify_logs (artist);
        create index sparkify_logs_userId on sparkify_logs (userId);
        create index sparkify_logs_status on sparkify_logs (status);
        create index sparkify_logs_user_agent_browser_family on sparkify_logs (userAgent_browser_family);
        create index sparkify_logs_user_agent_browser_version on sparkify_logs (userAgent_browser_version);
        create index sparkify_logs_user_agent_os_family on sparkify_logs (userAgent_os_family);
        create index sparkify_logs_user_agent_os_version on sparkify_logs (userAgent_os_version);
        create index sparkify_logs_user_agent_is_mobile on sparkify_logs (userAgent_is_mobile);
        create index sparkify_logs_user_agent_is_pc on sparkify_logs (userAgent_is_pc);
        create index sparkify_logs_user_agent_is_tablet on sparkify_logs (userAgent_is_tablet);
        create index sparkify_logs_lastName on sparkify_logs (lastName);
        create index sparkify_logs_method on sparkify_logs (method);
        create index sparkify_logs_registration_timestamp_btree on sparkify_logs using btree (registration_timestamp);
        create index sparkify_logs_registration_timestamp_brin on sparkify_logs using brin (registration_timestamp);
        create index sparkify_logs_song on sparkify_logs (song);
        create index sparkify_logs_level on sparkify_logs (level);
        create index sparkify_logs_firstName on sparkify_logs (firstName);
        create index sparkify_logs_auth on sparkify_logs (auth);
        create index sparkify_logs_gender on sparkify_logs (gender);
        create index sparkify_logs_page on sparkify_logs (page);
        
        -- NORMALLY WE WOULD NOT CREATE PARTITION TABLES BY HAND, 
        -- BUT FOR THE LOCAL SETUP THIS IS THE SIMPLEST SOLUTION.
        -- IN A PROD ENVIRONMENT PARTITIONS SHOULD BE MANAGED BY PARTMAN EXTENSION
        CREATE TABLE sparkify_logs_y2018m10d01 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-01') TO ('2018-10-02');
        CREATE TABLE sparkify_logs_y2018m10d02 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-02') TO ('2018-10-03');
        CREATE TABLE sparkify_logs_y2018m10d03 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-03') TO ('2018-10-04');
        CREATE TABLE sparkify_logs_y2018m10d04 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-04') TO ('2018-10-05');
        CREATE TABLE sparkify_logs_y2018m10d05 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-05') TO ('2018-10-06');
        CREATE TABLE sparkify_logs_y2018m10d06 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-06') TO ('2018-10-07');
        CREATE TABLE sparkify_logs_y2018m10d07 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-07') TO ('2018-10-08');
        CREATE TABLE sparkify_logs_y2018m10d08 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-08') TO ('2018-10-09');
        CREATE TABLE sparkify_logs_y2018m10d09 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-09') TO ('2018-10-10');
        CREATE TABLE sparkify_logs_y2018m10d10 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-10') TO ('2018-10-11');
        CREATE TABLE sparkify_logs_y2018m10d11 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-11') TO ('2018-10-12');
        CREATE TABLE sparkify_logs_y2018m10d12 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-12') TO ('2018-10-13');
        CREATE TABLE sparkify_logs_y2018m10d13 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-13') TO ('2018-10-14');
        CREATE TABLE sparkify_logs_y2018m10d14 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-14') TO ('2018-10-15');
        CREATE TABLE sparkify_logs_y2018m10d15 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-15') TO ('2018-10-16');
        CREATE TABLE sparkify_logs_y2018m10d16 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-16') TO ('2018-10-17');
        CREATE TABLE sparkify_logs_y2018m10d17 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-17') TO ('2018-10-18');
        CREATE TABLE sparkify_logs_y2018m10d18 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-18') TO ('2018-10-19');
        CREATE TABLE sparkify_logs_y2018m10d19 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-19') TO ('2018-10-20');
        CREATE TABLE sparkify_logs_y2018m10d20 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-20') TO ('2018-10-21');
        CREATE TABLE sparkify_logs_y2018m10d21 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-21') TO ('2018-10-22');
        CREATE TABLE sparkify_logs_y2018m10d22 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-22') TO ('2018-10-23');
        CREATE TABLE sparkify_logs_y2018m10d23 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-23') TO ('2018-10-24');
        CREATE TABLE sparkify_logs_y2018m10d24 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-24') TO ('2018-10-25');
        CREATE TABLE sparkify_logs_y2018m10d25 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-25') TO ('2018-10-26');
        CREATE TABLE sparkify_logs_y2018m10d26 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-26') TO ('2018-10-27');
        CREATE TABLE sparkify_logs_y2018m10d27 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-27') TO ('2018-10-28');
        CREATE TABLE sparkify_logs_y2018m10d28 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-28') TO ('2018-10-29');
        CREATE TABLE sparkify_logs_y2018m10d29 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-29') TO ('2018-10-30');
        CREATE TABLE sparkify_logs_y2018m10d30 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-30') TO ('2018-10-31');
        CREATE TABLE sparkify_logs_y2018m10d31 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-10-31') TO ('2018-11-01');
        
        CREATE TABLE sparkify_logs_y2018m11d01 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-01') TO ('2018-11-02');
        CREATE TABLE sparkify_logs_y2018m11d02 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-02') TO ('2018-11-03');
        CREATE TABLE sparkify_logs_y2018m11d03 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-03') TO ('2018-11-04');
        CREATE TABLE sparkify_logs_y2018m11d04 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-04') TO ('2018-11-05');
        CREATE TABLE sparkify_logs_y2018m11d05 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-05') TO ('2018-11-06');
        CREATE TABLE sparkify_logs_y2018m11d06 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-06') TO ('2018-11-07');
        CREATE TABLE sparkify_logs_y2018m11d07 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-07') TO ('2018-11-08');
        CREATE TABLE sparkify_logs_y2018m11d08 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-08') TO ('2018-11-09');
        CREATE TABLE sparkify_logs_y2018m11d09 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-09') TO ('2018-11-10');
        CREATE TABLE sparkify_logs_y2018m11d10 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-10') TO ('2018-11-11');
        CREATE TABLE sparkify_logs_y2018m11d11 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-11') TO ('2018-11-12');
        CREATE TABLE sparkify_logs_y2018m11d12 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-12') TO ('2018-11-13');
        CREATE TABLE sparkify_logs_y2018m11d13 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-13') TO ('2018-11-14');
        CREATE TABLE sparkify_logs_y2018m11d14 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-14') TO ('2018-11-15');
        CREATE TABLE sparkify_logs_y2018m11d15 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-15') TO ('2018-11-16');
        CREATE TABLE sparkify_logs_y2018m11d16 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-16') TO ('2018-11-17');
        CREATE TABLE sparkify_logs_y2018m11d17 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-17') TO ('2018-11-18');
        CREATE TABLE sparkify_logs_y2018m11d18 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-18') TO ('2018-11-19');
        CREATE TABLE sparkify_logs_y2018m11d19 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-19') TO ('2018-11-20');
        CREATE TABLE sparkify_logs_y2018m11d20 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-20') TO ('2018-11-21');
        CREATE TABLE sparkify_logs_y2018m11d21 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-21') TO ('2018-11-22');
        CREATE TABLE sparkify_logs_y2018m11d22 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-22') TO ('2018-11-23');
        CREATE TABLE sparkify_logs_y2018m11d23 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-23') TO ('2018-11-24');
        CREATE TABLE sparkify_logs_y2018m11d24 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-24') TO ('2018-11-25');
        CREATE TABLE sparkify_logs_y2018m11d25 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-25') TO ('2018-11-26');
        CREATE TABLE sparkify_logs_y2018m11d26 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-26') TO ('2018-11-27');
        CREATE TABLE sparkify_logs_y2018m11d27 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-27') TO ('2018-11-28');
        CREATE TABLE sparkify_logs_y2018m11d28 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-28') TO ('2018-11-29');
        CREATE TABLE sparkify_logs_y2018m11d29 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-29') TO ('2018-11-30');
        CREATE TABLE sparkify_logs_y2018m11d30 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-11-30') TO ('2018-12-01');
        
        CREATE TABLE sparkify_logs_y2018m12d01 PARTITION OF sparkify_logs FOR VALUES FROM ('2018-12-01') TO ('2018-12-02');
        '''
        self.database_cursor .execute(query)
        self.database_connection.commit()


if __name__ == '__main__':
    processor = Preprocessor()
    exitcode = processor.run()
    sys.exit(exitcode)
