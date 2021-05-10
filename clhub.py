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

from dataclasses import dataclass

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
    for k in ['call','txfreq','mode','timestamp',
                'exch1','exchange1',
                'mycall','operator','ID',
                'StationName','stationid','logger','app']:
        result[k] = getattr(root.find(f'./{k}'), 'text', None)

    if not result['txfreq']:
        logging.error('ERROR: Missing txfreq')
        return None

    result['freq'] = result['txfreq'][:-2]

    if not result['StationName']:
        result['StationName'] = result['stationid']

    if not result['operator']:
        result['operator'] = result['StationName']

    if not result['exchange1']:
        result['exchange1'] = result['exch1']

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
    for k in [('call',13),('freq',5),('timestamp',4),('mode',6),
                ('r_rpt',15)]:
        result[k[0]] = wt[k[1]]

    if not result['freq']:
        logging.error('ERROR: Missing freq')
        return None

    result['freq'] = result['freq'][:-1]

    if result['mode'] == '0':
        result['mode'] = 'CW'
        result['exchange1'] = result['r_rpt'][3:]
    else:
        result['mode'] = 'SSB'
        result['exchange1'] = result['r_rpt'][2:]


    result['timestamp'] = datetime.utcfromtimestamp(int(result['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
    result['ID'] = 'WT' + ''.join(random.choices(string.ascii_letters + string.digits, k=14))
    result['app'] = 'WinTest'

    return result

@dataclass
class Band:
    name: str
    fmin: int   # kHz
    fmax: int   # kHz

BANDS = [
    Band('160', 1_800,  2_000),
    Band('80',  3_500,  3_800),
    Band('40',  7_000,  7_200),
    Band('30', 10_100, 10_150),
    Band('20', 14_000, 14_350),
    Band('17', 18_068, 18_168),
    Band('15', 21_000, 21_450),
    Band('12', 24_890, 24_990),
    Band('10', 28_000, 30_000),
]


def freq2band(freq):
    for b in BANDS:
        if b.fmin <= freq <= b.fmax:
            return b.name
    return '???'


def store_qso(qso):
    global conn
    logging.info(f'Storing {qso}')
    try:
        freq = int(qso['freq'])
    except:
        logging.error('ERROR: Bad freq')
        return

    band = freq2band(freq)

    c = conn.cursor()
    c.execute("""insert into qso (id, timestamp, caller, `mode`, freq,
                    band, mycall, exchange, operator,
                    station, logger,
                    from_ip, from_time)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (qso['ID'], qso['timestamp'], qso['call'], qso['mode'], freq,
                    band, qso['mycall'], qso['exchange1'], qso['operator'],
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

