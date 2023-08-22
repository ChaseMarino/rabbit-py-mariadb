"""
Calc calcexecutor  :
    job:
    inputs:

    outputs:

http://pyclips.sourceforge.net/web/?q=node/25
"""
import os
import sys
import json
import time
import uuid
import signal
import logging  # type: ignore
import string
import random
import hashlib
import traceback
from decimal import *
import mariadb
import mariadb.constants.CLIENT as CLIENT
from datetime import datetime
import pika  # type: ignore
# from confluent_kafka import Consumer, OFFSET_BEGINNING, KafkaError
from autologging import logged, traced, TRACE  # type: ignore
from prometheus_client import start_http_server, Summary, Counter


class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True



# BOOTSTRAP_SERVER = os.environ.get('BOOTSTRAP_SERVER', 'localhost:9092')
ITEM_ID = os.environ.get('ITEM_ID', '')
if ITEM_ID == '':
    raise Exception("ITEM_ID Required")

DEBUG = os.environ.get('DEBUG', 0) # 0 = False, anyother = True

MARIDB_SERVER = os.environ.get('MARIDB_SERVER', '127.0.0.1')
MARIDB_USER = os.environ.get('MARIDB_USER', 'user')
MARIDB_PASSWORD = os.environ.get('MARIDB_PASSWORD', 'password')
MARIDB_PORT = int(os.environ.get('MARIDB_PORT', 3306))
MARIDB_DB = os.environ.get('MARIDB_DB', 'db')

RABBIT_SERVER = os.environ.get('RABBIT_SERVER', 'rabbit')
RABBIT_USER = os.environ.get('RABBIT_USER', 'guest')
RABBIT_PASSWORD = os.environ.get('RABBIT_PASSWORD', 'guest')
RABBIT_PORT = os.environ.get('RABBIT_PORT', 5672)
RABBIT_VHOST = os.environ.get('RABBIT_VHOST', "")


# Def metrics
messages_processed = Counter('calcs_messages_processed', 'Num of messages entering handler') #
message_errors = Counter('calcs_message_errors', 'Num of kafka messages in error state') #
process_time = Summary('process_time', 'Message process time counter/time')


logging.basicConfig(level=TRACE, stream=sys.stdout,
                    format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)

logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

def myhash(s):
    x = hashlib.md5(s.encode())
    return x.hexdigest()

@process_time.time()
def processmessage(cursor, document):
    tic = time.perf_counter()
    json_document = json.loads(document)

    PLANETID = json_document["planetCode"]
    GALAXYID = json_document["galaxyCode"]
    ORBITCYCLE = json_document["orbitBatchId"]

    HHK_GALAXY = myhash("|{}|".format(GALAXYID))
    HHK_PLANET = myhash("|{}|".format(PLANETID))
    HHK_SATELLITE = myhash("|{}|".format(PLANETID))

    try:

        for star in json_document['stars']:
            # From document
            ORBITID = star['orbitSequence']
            SATELLITEID = star['orbitSequence']

            STARID = star['starCode']

            # Calculated
            HHK_ORBIT_CYCLE = myhash("|{}|{}|".format(ORBITCYCLE, ORBITID))
            HHK_SATELLITE = myhash("|{}|".format("TEST SATELLITE ID"))
            HHK_STAR_ENTITY = myhash("|{}|".format(STARID))
            LHK_PLANET_SATELLITE = myhash("|{}|{}|{}|{}|{}|{}|".format(GALAXYID,
                                                                        STARID,
                                                                        PLANETID,
                                                                        ORBITCYCLE,
                                                                        ORBITID,
                                                                        SATELLITEID))
            sSQL = "\n"

            sSQL += "INSERT INTO LINK_PLANET_SATELLITES (LHK_PLANET_SATELLITE, HHK_GALAXY, HHK_STAR_ENTITY, HHK_PLANET, "
            sSQL += "                                     HHK_ORBIT_CYCLE, HHK_SATELLITE, BK_GALAXY, BK_STAR_CODE, BK_PLANET, "
            sSQL += "                                     BK_ORBIT_DATE, BK_ORBIT_NO, BK_SATELLITE_ID, SOURCE_UNIVERSE) \n"
            sSQL += """ SELECT '{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}', {}, '{}','{}'
                        FROM DUAL
                        WHERE NOT EXISTS (SELECT *
                                                FROM LINK_PLANET_SATELLITES
                                                WHERE LHK_PLANET_SATELLITE = '{}' );\n""".format(LHK_PLANET_SATELLITE,
                                                                                                HHK_GALAXY,
                                                                                                HHK_STAR_ENTITY,
                                                                                                HHK_PLANET,
                                                                                                HHK_ORBIT_CYCLE,
                                                                                                HHK_SATELLITE,
                                                                                                GALAXYID,
                                                                                                STARID,
                                                                                                PLANETID,
                                                                                                ORBITCYCLE,
                                                                                                ORBITID,
                                                                                                SATELLITEID,
                                                                                                'universeRecorder',
                                                                                                LHK_PLANET_SATELLITE
                                                                                                  )

            sSQL = sSQL[:-2] + ";\n"
            cursor.execute(sSQL)

            sSQL = "\n"

            logging.info("processmessage.Total Time {}".format(time.perf_counter() - tic))

            print("inserted all star data")

    except Exception as ex:
        logging.error("Fatal error in main Process loop processmessage", exc_info=True)
        logging.error(sSQL)
        raise ex


@logged
def rq_callback(channel, method, properties, body):  # pylint: disable-msg=W0613
    """
        Rabbit MQ Callback
    """
    logging.info("rq_callback {}:{}".format(method.routing_key, len(body)))  # pylint: disable-msg=W1202
    try:
        conn = mariadb.connect(
                                user=MARIDB_USER,
                                password=MARIDB_PASSWORD,
                                host=MARIDB_SERVER,
                                port=MARIDB_PORT,
                                database=MARIDB_DB,
                                autocommit=False
                               )
        # ,client_flag=CLIENT.MULTI_STATEMENTS
        cursor = conn.cursor()
        processmessage(cursor, body)
        conn.commit()
        messages_processed.inc()
        logging.info("Sending Ack")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as ex:   # pylint: disable-msg=W0703
        message_errors.inc()
        logging.exception(ex)
        logging.info("Sending Nack")
        channel.basic_nack(delivery_tag=method.delivery_tag)

@logged
def run():
    killer = GracefulKiller()
    """
        Main Entry Point into System
    """
    logging.info("runit info")
    s_url = "amqp://{}:{}@{}:{}/{}".format(RABBIT_USER,
                                            RABBIT_PASSWORD,
                                            RABBIT_SERVER,
                                            RABBIT_PORT,
                                            RABBIT_VHOST)
    parameters = pika.URLParameters(s_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    queue_name = 'results-{}'.format(ITEM_ID)
    channel.exchange_declare(exchange='ipn.results', exchange_type='topic', durable=True)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange='ipn.results',
                       queue=queue_name,
                       routing_key="tenant.{}".format(ITEM_ID))
    logging.info(queue_name)
    channel.basic_consume(on_message_callback=rq_callback, queue=queue_name)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    logging.info("in __main__")
    if DEBUG:
        # Debug code to test the ProcessMessage function
        with open('result.json','r') as f:
            sjson = f.read()
            conn = mariadb.connect(
                                    user=MARIDB_USER,
                                    password=MARIDB_PASSWORD,
                                    host=MARIDB_SERVER,
                                    port=MARIDB_PORT,
                                    database=MARIDB_DB,
                                    autocommit=False
                                   )
            # ,client_flag=CLIENT.MULTI_STATEMENTS
            cursor = conn.cursor()
            processmessage(cursor, sjson)
            conn.commit()

    # start_http_server(8000)
    # run()
