#!/usr/bin/python3
import argparse
import glob
import logging
import subprocess
import os

import gzip
import environ

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import datetime


env = environ.Env()
environ.Env.read_env()

BACKUP_PATH = env('BACKUP_PATH')


def list_available_backup(all_file=False):
    path = rf'{BACKUP_PATH}*residence.dump.gz'
    list_all_file = glob.glob(path)

    if all_file is True:
        return list_all_file

    dates_keys = []
    for file in list_all_file:
        # ['/tmp/backup', '20230518', '220813', 'residence.dump']
        dates_keys.append(file.split('-')[1])
    return dates_keys


def get_backup_file(file_date):
    path = rf'{BACKUP_PATH}backup-{file_date}*.gz'
    file = glob.glob(path)[0]
    return file


def extract_file(src_file):
    extracted_file, extension = os.path.splitext(src_file)
    with gzip.open(src_file, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    print(extracted_file)
    return extracted_file


def compress_file(src_file):
    compressed_file = "{}.gz".format(str(src_file))
    with open(src_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)

    os.remove(src_file)
    return compressed_file


def backup_postgres_db(host, database_name, port, user, password,
                       dest_file):
    """
    Backup postgres db to a file.
    """
    try:
        process = subprocess.Popen(
            ['pg_dump',
             '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
             '-Fc',
             '-f', dest_file,
             '-v'],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print(f'Command failed. Return code : {process.returncode}')
            exit(1)
        return output
    except Exception as e:
        print(e)
        exit(1)


def restore_postgres_db(db_host, db, port, user, password, backup_file):
    """
    Restore postgres db from a file.
    """

    try:
        print(user, password, db_host, port, db)
        process = subprocess.Popen(
            ['pg_restore',
             '--no-owner',
             '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user,
                                                           password,
                                                           db_host,
                                                           port, db),
             '-v',
             backup_file],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print(f'Command failed. Return code : {process.returncode}')

        return output
    except Exception as e:
        print("Issue with the db restore : {}".format(e))


def create_db(db_host, database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)

    except Exception as e:
        print(e)
        exit(1)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        if database != env('PSQL_DATABASE'):
            cur.execute("DROP DATABASE {} ;".format(database))
    except Exception as e:
        print('DB does not exist, nothing to drop')
    cur.execute(f"CREATE DATABASE {database} ;")
    cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {database} TO {user_name};")
    return database


def swap_restore_active(db_host, restore_database, active_database, db_port,
                        user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(
            f'''
            SELECT pg_terminate_backend( pid ) FROM pg_stat_activity 
            WHERE pid <> pg_backend_pid( ) AND datname = '{active_database}'
            '''
        )

        cur.execute("DROP DATABASE {}".format(active_database))
        cur.execute(
            f'''
            ALTER DATABASE "{restore_database}" RENAME TO "{active_database}";
            '''
        )
    except Exception as e:
        print(e)
        exit(1)


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args_parser = argparse.ArgumentParser(
        description='Postgres database management'
    )
    args_parser.add_argument("--action",
                             metavar="action",
                             choices=['list', 'restore', 'backup', 'active'],
                             required=True)
    args_parser.add_argument("--date",
                             metavar="YYYY-MM-dd",
                             help="Date to use for restore (show with --action list)")
    args = args_parser.parse_args()

    postgres_host = env('PSQL_HOST')
    postgres_port = env('PSQL_PORT')
    postgres_db = env('PSQL_DATABASE')
    postgres_restore = "{}_restore".format(postgres_db)
    postgres_user = env('PSQL_USER')
    postgres_password = env('PSQL_PASSWORD')
    timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = 'backup-{}-{}.dump'.format(timestr, postgres_db)
    local_file_path = '{}{}'.format(BACKUP_PATH, filename)

    # backup task
    if args.action == "backup":
        logger.info(
            'Backing up {} database to {}'.format(postgres_db, local_file_path)
        )
        result = backup_postgres_db(postgres_host,
                                    postgres_db,
                                    postgres_port,
                                    postgres_user,
                                    postgres_password,
                                    local_file_path)
        for line in result.splitlines():
            logger.info(line)

        logger.info("Backup complete")
        logger.info("Compressing {}".format(local_file_path))
        compress_file(local_file_path)

    # restore task
    elif args.action == "restore":
        if not args.date:
            logger.warn(
                'No date was chosen for restore. Run again with the "list" '
                'action to see available restore dates'
            )
        else:
            all_backup_keys = list_available_backup()
            receive_date = args.date.replace('-', '')
            backup_match = [d for d in all_backup_keys if receive_date == d]
            if backup_match:
                logger.info(
                    "Found the following backup : {}".format(backup_match)
                )
            else:
                logger.error(
                    "No match found for backups with "
                    "date : {}".format(args.date)
                )
                logger.info(
                    "Available keys : {}".format([d for d in all_backup_keys])
                )
                exit(1)

            file_backup = get_backup_file(backup_match[0])
            logger.info(f"File backup is {file_backup}")
            logger.info(
                "Creating temp database for restore "
                ": {}".format(postgres_restore)
            )
            tmp_database = create_db(postgres_host,
                                     postgres_restore,
                                     postgres_port,
                                     postgres_user,
                                     postgres_password
                                     )
            logger.info(
                "Created temp database for restore : {}".format(tmp_database)
            )
            logger.info("Restore starting")
            dump_file = extract_file(file_backup)
            result = restore_postgres_db(postgres_host,
                                         postgres_restore,
                                         postgres_port,
                                         postgres_user,
                                         postgres_password,
                                         dump_file
                                         )
            for line in result.splitlines():
                logger.info(line)
            logger.info("Restore complete")

    # list backup
    elif args.action == "list":
        backup_files = list_available_backup(all_file=True)
        logger.info(f"Backup files: {backup_files}")

    elif args.action == 'active':
        restored_db_name = postgres_db
        logger.info(
            f'''
            Switching restored database with active one : 
            {postgres_restore} -> {restored_db_name}"
            '''
        )
        swap_restore_active(postgres_host,
                            postgres_restore,
                            restored_db_name,
                            postgres_port,
                            postgres_user,
                            postgres_password)
        logger.info("Database restored and active.")


if __name__ == '__main__':
    main()

