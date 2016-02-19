#!/usr/bin/env python3

import datetime
import json
import os
import time

import stomp

from pprint import pprint
from collections import OrderedDict
from enum import Enum

import logging

import operating_companies
import locations


HOSTNAME = 'datafeeds.networkrail.co.uk'
CHANNEL = 'TRAIN_MVT_ALL_TOC'  # See http://nrodwiki.rockshore.net/index.php/Train_Movements
LOG = None


def JsonSerializer(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial

    elif isinstance(obj, Enum):
        return obj.name

    elif hasattr(obj, 'serialize'):
        return obj.serialize()

    raise TypeError("Type `{}` not serializable".format(type(obj)))


class VariationStatus(Enum):
    """
    One of "ON TIME", "EARLY", "LATE" or "OFF ROUTE"
    """
    on_time = 1
    early = 2
    late = 3
    off_route = 4

    @classmethod
    def get(cls, string):
        return {
            'ON TIME': VariationStatus.on_time,
            'EARLY': VariationStatus.early,
            'LATE': VariationStatus.late,
            'OFF ROUTE': VariationStatus.off_route,
        }[string]


class EventType(Enum):
    """
    One of "ARRIVAL", "DEPARTURE" or "DESTINATION"
    """
    arrival = 1
    departure = 2
    destination = 3

    @classmethod
    def get(cls, string):
        return {
            'ARRIVAL': EventType.arrival,
            'DEPARTURE': EventType.departure,
            'DESTINATION': EventType.destination,
        }[string]


class TrainMovementsMessage(object):
    """
    Decodes and represents a Train Movements Message. The raw message looks
    something like:
    ```
    {
        "status": "LATE",
        "planned_timestamp": "1455883470000",
        "event_type": "DEPARTURE",
        "train_terminated": "false",
        "direction_ind": "UP",
        "toc_id": "88",
        "auto_expected": "true",
        "event_source": "AUTOMATIC",
        "reporting_stanox": "87701",
        "gbtt_timestamp": "1455883440000",
        "platform": " 1",
        "correction_ind": "false",
        "original_loc_stanox": "",
        "planned_event_type": "DEPARTURE",
        "timetable_variation": "2",
        "delay_monitoring_point": "true",
        "line_ind": "F",
        "next_report_stanox": "87700",
        "train_id": "892A39MI19",
        "offroute_ind": "false",
        "current_train_id": "",
        "loc_stanox": "87701",
        "next_report_run_time": "1",
        "route": "2",
        "train_file_address": null,
        "division_code": "88",
        "actual_timestamp": "1455883560000",
        "original_loc_timestamp": "",
        "train_service_code": "24745000"
    },
    ```
    """

    def __init__(self, raw):
        self.raw = raw
        self._validate_assumptions()

    def _validate_assumptions(self):
        assert self.division_code == self.operating_company

    def __str__(self):
        return json.dumps(self.serialize(), indent=4, default=JsonSerializer)

    @property
    def planned_event_type(self):
        return EventType.get(self.raw['planned_event_type'])

    @property
    def event_type(self):
        EventType.get(self.raw['event_type'])

    @property
    def status(self):
        return VariationStatus.get(self.raw['variation_status'])

    @property
    def planned_datetime(self):
        return self._decode_timestamp(self.raw['planned_timestamp'])

    @property
    def actual_datetime(self):
        return self._decode_timestamp(self.raw['actual_timestamp'])

    @property
    def planned_timetable_datetime(self):
        return self._decode_timestamp(self.raw['gbtt_timestamp'])

    @property
    def location(self):
        """
        The location on the rail network at which this event happened.
        """
        return self._decode_stanox(self.raw['loc_stanox'])

    @property
    def location_stanox(self):
        return self.raw['loc_stanox']

    @property
    def is_correction(self):
        return self._decode_boolean(self.raw['correction_ind'])

    @property
    def train_terminated(self):
        """
        Set to "true" if the train has completed its journey, or "false"
        otherwise.
        """
        return self._decode_boolean(self.raw['train_terminated'])

    @property
    def operating_company(self):
        """
        """
        return self._decode_operating_company(self.raw['toc_id'])

    @property
    def division_code(self):
        """
        Operating company ID as per TOC Codes
        """
        return self._decode_operating_company(self.raw['division_code'])

    @property
    def train_service_code(self):
        """
        Train service code as per schedule
        eg. "24745000"
        """
        return self.raw['train_service_code']

    @property
    def train_id(self):
        """
        The 10-character unique identity for this train at TRUST activation
        time
        """
        return self.raw['train_id']

    @property
    def is_off_route(self):
        """
        Set to False if this report is for a location in the schedule, or
        True if it is not.
        """
        return self._decode_boolean(self.raw['offroute_ind'])

    @property
    def current_train_id(self):
        """
        Where a train has had its identity changed, the current 10-character
        unique identity for this train.
        """
        return self.raw['current_train_id']

    @property
    def original_location(self):
        """
        If the location has been revised, the location in the schedule at
        activation time
        """
        return self._decode_stanox(self.raw['original_loc_stanox'])

    @property
    def original_location_planned_departure_datetime(self):
        """
        The planned departure time associated with the original location
        """
        return self._decode_timestamp(self.raw['original_loc_timestamp'])

    @property
    def direction(self):
        """
        For automatic reports, either "UP" or "DOWN" depending on the direction
        of travel
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['direction_ind'])

    @property
    def auto_expected(self):
        """
        Set to "true" if an automatic report is expected for this location,
        otherwise "false"
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['auto_expected'])

    @property
    def event_source(self):
        """
        Whether the event source was "AUTOMATIC" from SMART, or "MANUAL" from
        TOPS or TRUST SDR
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['event_source'])

    @property
    def reporting_location(self):
        """
        The location that generated this report. Set to "00000" for manual and
        off-route reports.
        """
        raise NotImplementedError()
        # return self._decode_stanox(self.raw['reporting_stanox'])

    @property
    def platform(self):
        """
        Two characters (including a space for a single character) or blank if
        the movement report is associated with a platform number
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['platform'])

    @property
    def timetable_variation(self):
        """
        The number of minutes variation from the scheduled time at this
        location. Off-route reports will contain "0"
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['timetable_variation'])

    @property
    def delay_monitoring_point(self):
        """
        Set to True if this is a delay monitoring point, False if it is
        not. Off-route reports will contain False.
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['delay_monitoring_point'])

    @property
    def line_ind(self):
        """
        A single character (or blank) depending on the line the train is
        travelling on, e.g. F = Fast, S = Slow
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['line_ind'])

    @property
    def next_report_location(self):
        """
        The location at which the next report for this train is due
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['next_report_stanox'])

    @property
    def next_report_run_time(self):
        """
        The running time to the next location.
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['next_report_run_time'])

    @property
    def route(self):
        """
        A single character (or blank) to indicate the exit route from this
        location
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['route'])

    @property
    def train_file_address(self):
        """
        The TOPS train file address, if applicable.
        """
        raise NotImplementedError()
        # return self._decode_???(self.raw['train_file_address'])

    @property
    def early_late_description(self):
        if not self.actual_datetime or not self.planned_datetime:
            return '[unknown]'

        mins_late = (
            self.actual_datetime - self.planned_datetime).total_seconds() / 60

        if self.status is VariationStatus.late:
            return '{} mins late'.format(mins_late)

        elif self.status is VariationStatus.early:
            return '{} mins early'.format(-mins_late)

        elif self.status is VariationStatus.on_time:
            return 'on time'

    def serialize(self):
        field_names = [
            'planned_event_type',
            'status',
            'planned_datetime',
            'actual_datetime',
            'planned_timetable_datetime',
            'early_late_description',
            'location',
            'location_stanox',
            'operating_company',
            'is_correction',
        ]
        return OrderedDict(
            [(name, getattr(self, name)) for name in field_names])

    @staticmethod
    def _decode_boolean(string):
        if string == 'true':
            return True
        elif string == 'false':
            return False
        raise ValueError('Invalid boolean: `{}`'.format(string))

    @staticmethod
    def _decode_stanox(stanox):
        return locations.from_stanox(stanox)

    @staticmethod
    def _decode_operating_company(numeric_code):
        """
        eg: "88"
        """
        if numeric_code == '00':
            return None

        return operating_companies.from_numeric_code(int(numeric_code))

    @staticmethod
    def _decode_timestamp(string):
        """
        Timestamp appears to be in milliseconds:
        `1455887700000` : Tue, 31 Mar in the year 48105.
        `1455887700`    : Fri, 19 Feb 2016 13:15:00 GMT
        """

        if string == '':
            return None

        try:
            return datetime.datetime.fromtimestamp(int(string) / 1000)
        except ValueError as e:
            raise ValueError('Choked on `{}`: {}'.format(string, repr(e)))


class TrainMovementsListener(object):
    def on_error(self, headers, message):
        LOG.error("ERROR: {} {}".format(headers, message))

    def on_message(self, headers, messages):
        LOG.debug('STOMP headers {}, message: {}'.format(headers, messages))

        for message in json.loads(messages):
            self._handle_message(message)

    def _handle_message(self, raw_message):
        """
        Train movement message comprises a `header` and a `body`. The `header`
        http://nrodwiki.rockshore.net/index.php/Train_Movement

        """
        header = raw_message['header']

        if not self._validate_header(header):
            LOG.debug('Dropping invalid message due to header')
            return

        decoded = TrainMovementsMessage(raw_message['body'])
        print(decoded)
        return

        stanox = body.get('loc_stanox')
        if stanox == '72410':  # euston
            print("\n**** EUSTON ****\n")
            pprint(message)
        else:
            print("somewhere else ({})".format(stanox))

    @staticmethod
    def _validate_header(header):
        """
        ```
        "header": {
            "user_id": "",
            "msg_type": "0003",
            "msg_queue_timestamp": "1455883630000",
            "source_dev_id": "",
            "original_data_source": "SMART",
            "source_system_id": "TRUST"
        }
        ```
        """

        if header['msg_type'] != '0003':
            LOG.error('Message type was `{}`, expected `0003`. This "cannot" '
                      'happen'.format(header['msg_type']))
            return False

        return True


def main():
    logging.basicConfig(level=logging.INFO)
    global LOG
    LOG = logging.getLogger('')

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
