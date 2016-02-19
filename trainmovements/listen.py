#!/usr/bin/env python

import os
import json
import time

import stomp

from pprint import pprint

import logging


class MyListener(object):
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
    hostname = 'datafeeds.networkrail.co.uk'

    username = os.environ['NR_DATAFEEDS_USERNAME']
    password = os.environ['NR_DATAFEEDS_PASSWORD']

    channel = 'TRAIN_MVT_ALL_TOC'

    conn = stomp.Connection(host_and_ports=[(hostname, 61618)])
    conn.set_listener('mylistener', MyListener())
    conn.start()
    conn.connect(username=username, passcode=password)

    conn.subscribe(destination='/topic/{}'.format(channel), id=1, ack='auto')

    keep_running = True
    try:
        while keep_running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Quitting.")
        keep_running = False
    else:
        raise

    conn.disconnect()

if __name__ == '__main__':
    main()
