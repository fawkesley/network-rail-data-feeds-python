#!/usr/bin/env python

import os
import json
import time

import stomp

from pprint import pprint

import logging

HOSTNAME = 'datafeeds.networkrail.co.uk'
CHANNEL = 'TRAIN_MVT_ALL_TOC'


class TrainMovementsListener(object):
    def on_error(self, headers, message):
        print("ERROR: {} {}".format(headers, message))

    def on_message(self, headers, message):
        #print("MESSAGE: {} {}".format(headers, message))
        data = json.loads(message)
        for event in data:
            self._handle_event(event)

    def _handle_event(self, data):

        stanox = data['body'].get('loc_stanox')
        if stanox == '72410':  # euston
            print("\n**** EUSTON ****\n")
            pprint(data)
        else:
            print("somewhere else ({})".format(stanox))


def main():
    logging.basicConfig(level=logging.WARN)

    username = os.environ['NR_DATAFEEDS_USERNAME']
    password = os.environ['NR_DATAFEEDS_PASSWORD']

    conn = create_data_feed_connection(HOSTNAME, username, password, CHANNEL)

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print("Quitting.")
            break

    conn.disconnect()


def create_data_feed_connection(hostname, username, password, channel):
    conn = stomp.Connection(host_and_ports=[(hostname, 61618)])
    conn.set_listener('mylistener', TrainMovementsListener())
    conn.start()
    conn.connect(username=username, passcode=password)

    conn.subscribe(destination='/topic/{}'.format(channel), id=1, ack='auto')
    return conn

if __name__ == '__main__':
    main()
