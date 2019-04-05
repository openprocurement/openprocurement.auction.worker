# -*- coding: utf-8 -*-

from gevent import monkey
monkey.patch_all()

import os.path
import datetime
import json
import sys
import argparse
import contextlib
import tempfile
from dateutil.tz import tzlocal
from pkg_resources import iter_entry_points
from gevent.subprocess import check_output, sleep
from uuid import uuid4

from openprocurement.auction.tests.main import update_auctionPeriod


PWD = os.path.dirname(os.path.realpath(__file__))
CWD = os.getcwd()

def run_simple(tender_file_path):
    auction_id = uuid4().hex
    with update_auctionPeriod(tender_file_path, auction_type='simple') as auction_file:
        check_output(TESTS['simple']['worker_cmd'].format(CWD, auction_id, auction_file).split())
    sleep(30)
    return auction_id

def run_multilot(tender_file_path):
    with open(tender_file_path) as _file:
        data = json.load(_file).get('data', {})
        auction_id = data.get('id')
        lot_id = data.get('lots', [])[0].get('id')
    with update_auctionPeriod(tender_file_path, auction_type='multilot') as auction_file:
        command_line = TESTS['multilot']['worker_cmd'].format(
            CWD, auction_id, auction_file, lot_id
        )
        check_output(command_line.split())
    sleep(30)


TESTS = {
    "simple": {
        "worker_cmd": '{0}/bin/auction_worker planning {1}'
                      ' {0}/etc/auction_worker_defaults.yaml'
                      ' --planning_procerude partial_db --auction_info {2}',
        "runner": run_simple,
        'auction_worker_defaults': 'auction_worker_defaults:{0}/etc/auction_worker_defaults.yaml',
        'suite': PWD
    },
    "multilot": {
        "worker_cmd": '{0}/bin/auction_worker planning {1}'
                      '{0}/etc/auction_worker_defaults.yaml'
                      ' --planning_procerude partial_db --auction_info {2} --lot {3}',
        "runner": run_multilot,
        'auction_worker_defaults': 'auction_worker_defaults:{0}/etc/auction_worker_defaults.yaml',
        "suite": PWD
    }
}


def includeme(tests):
    tests.update(TESTS)
