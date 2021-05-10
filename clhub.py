#!/usr/bin/python3

import sys
import argparse
import logging
import gzip
import socketserver

import xml.etree.ElementTree as ET

from io import StringIO
import csv
from datetime import datetime, timezone
import string
import random

import MySQLdb


def parse_n1mm(xml):
    global logging

    n = len(xml)
    for i in reversed(range(n)):
        if xml[i] == ord('>'):
            xml = xml[0:i+1]
            break

    try:
        root = ET.fromstring(xml)
    except:
        logging.error('ERROR: Invalid XML')
        return None

    result = {'format': 'N1MM'}
    for k in ['call','band','txfreq','mode','timestamp',
                'mycall','operator','ID',
                'StationName','stationid','logger','app']:
        result[k] = getattr(root.find(f'./{k}'), 'text', None)

    result['freq'] = result['txfreq']

    if not result['StationName']:
        result['StationName'] = result['stationid']

    if not result['app']:
        result['app'] = result['logger']

    return result


def parse_wintest(data):
    data = data[0:len(data)-3]
    f = StringIO(str(data, 'latin1'))
    reader = csv.reader(f, delimiter=' ')
    wt = list(reader)[0]
    logging.debug(wt)

    result = {'format': 'WinTest'}
    for k in [('call',13),('freq',5),('timestamp',4)]:
        result[k[0]] = wt[k[1]]

    result['timestamp'] = datetime.utcfromtimestamp(int(result['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
    result['ID'] = 'WT' + ''.join(random.choices(string.ascii_letters + string.digits, k=14))
    result['app'] = 'WinTest'

    return result


def store_qso(qso):
    global conn
    logging.info(f'Storing {qso}')
    try:
        freq = int(qso['freq'])
    except:
        logging.error('ERROR: Bad freq')
        return

    c = conn.cursor()
    c.execute("""insert into qso (id, timestamp, `call`, `mode`, freq,
                    mycall, operator,
                    station, logger,
                    from_ip, from_time)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (qso['ID'], qso['timestamp'], qso['call'], qso['mode'], freq,
                    qso['mycall'], qso['operator'],
                    qso['StationName'], qso['app'],
                    qso['from_ip'], qso['from_time'])
            )
    conn.commit()
    c.close()
    

class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # self.request is the TCP socket connected to the client
        ip = self.client_address[0]
        raw = self.request.recv(1024).strip()
        logging.info(f'*** {ip} wrote: [{len(raw)} compressed bytes]')
        try:
            data = gzip.decompress(raw)
        except:
            logging.error('ERROR: Not gzipped data')
            return

        logging.debug(data)
        logging.debug(f'[{len(data)} bytes]')

        qso = None
        if b'<contactinfo>' in data:
            qso = parse_n1mm(data)
        elif b'ADDQSO:' in data:
            qso = parse_wintest(data)
        else:
            logging.info('???')

        if not qso:
            return

        qso['from_ip'] = ip
        qso['from_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        store_qso(qso)

#########################

def process_args():
    parser = argparse.ArgumentParser(description='ClubLog hub server')
    parser.add_argument('-d', '--debug', action='store_true',
                    help='debug log level')
    parser.add_argument('--tcp-host', metavar='HOST', type=str, default='0.0.0.0',
                    help='TCP server host/IP (default: 0.0.0.0)')
    parser.add_argument('--tcp-port', metavar='PORT', type=int, default=8432,
                    help='TCP server port (default: 8432')
    parser.add_argument('--db-host', metavar='HOST', type=str, default='localhost',
                    help='MySQL DB host (default: localhost)')
    parser.add_argument('--db-port', metavar='PORT', type=int, default=3306,
                    help='MySQL DB server port (default: 3306)')
    parser.add_argument('--db-user', metavar='USER', type=str, required=True, 
                    help='MySQL DB user')
    parser.add_argument('--db-password', metavar='PW', type=str,
                    help='MySQL DB password (default: None)')
    parser.add_argument('--db-name', metavar='DB', type=str, required=True, 
                    help='MySQL DB name')

    parsed_args, unparsed_args = parser.parse_known_args()
    if unparsed_args:
        parser.print_help()
        sys.exit(1)

    return parsed_args

#########################

args = process_args()

log_level = logging.INFO
if args.debug:
    log_level = logging.DEBUG

logging.basicConfig(format='%(asctime)s %(message)s',\
            filename='clhub.log', level=log_level)

conn = MySQLdb.connect(user=args.db_user, passwd=args.db_password, db=args.db_name,
        host=args.db_host, port=args.db_port)

logging.info(f'Listening on {args.tcp_host}:{args.tcp_port}')

with socketserver.TCPServer((args.tcp_host, args.tcp_port), MyTCPHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()

