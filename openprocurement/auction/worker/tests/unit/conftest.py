# -*- coding: utf-8 -*-
import datetime
import logging
import os
import pytest
import yaml
import couchdb

from flask import redirect
from mock import MagicMock
from dateutil.tz import tzlocal
from datetime import timedelta
from StringIO import StringIO
from pytz import timezone as tz

from openprocurement.auction.worker.auction import Auction, SCHEDULER
from openprocurement.auction.worker.mixins import LOGGER
from openprocurement.auction.worker.tests.data.data import (
    tender_data, lot_tender_data, features_tender_data, test_auction_document
)
from openprocurement.auction.worker.server import (
    app as worker_app, BidsForm
)

COUCHDB_HTTP_HOST = os.environ.get('COUCHDB_HTTP_HOST', '127.0.0.1')
COUCHDB_HTTP_PORT = os.environ.get('COUCHDB_HTTP_PORT', '9000')
COUCHDB_USER = os.environ.get('COUCHDB_USER', 'admin')
COUCHDB_PASSWORD = os.environ.get('COUCHDB_PASSWORD', 'zaq1xsw2')


def update_auctionPeriod(data):
    new_start_time = (datetime.datetime.now(tzlocal()) + datetime.timedelta(seconds=1)).isoformat()
    if 'lots' in data['data']:
        for lot in data['data']['lots']:
            lot['auctionPeriod']['startDate'] = new_start_time
    data['data']['auctionPeriod']['startDate'] = new_start_time



PWD = os.path.dirname(os.path.realpath(__file__))

worker_defaults_file_path = os.path.join(PWD, "../data/auction_worker_defaults.yaml")
with open(worker_defaults_file_path) as stream:
    worker_defaults = yaml.load(stream)
    if COUCHDB_USER and COUCHDB_PASSWORD:
        couchdb_template_url = 'http://{0}:{1}@{2}:{3}/database'
    else:
        couchdb_template_url = 'http://{2}:{3}/database'
    worker_defaults['COUCH_DATABASE'] = 'http://{}:{}@{}:{}/database'.format(
        COUCHDB_USER,
        COUCHDB_PASSWORD,
        COUCHDB_HTTP_HOST,
        COUCHDB_HTTP_PORT
    )



@pytest.yield_fixture(
    scope="function",
    params=[
        {'tender_data': tender_data, 'lot_id': False},
        {'tender_data': lot_tender_data, 'lot_id': lot_tender_data['data']['lots'][0]['id']}
    ],
    ids=['simple', 'multilot']
)
def universal_auction(request):
    update_auctionPeriod(request.param['tender_data'])

    yield Auction(
        tender_id=request.param['tender_data']['data']['auctionID'],
        worker_defaults=yaml.load(open(worker_defaults_file_path)),
        auction_data=request.param['tender_data'],
        lot_id=request.param['lot_id']
    )

@pytest.yield_fixture(scope="function")
def auction():
    update_auctionPeriod(tender_data)

    yield Auction(
        tender_id=tender_data['data']['auctionID'],
        worker_defaults=yaml.load(open(worker_defaults_file_path)),
        auction_data=tender_data,
        lot_id=False
    )


@pytest.yield_fixture(scope="function")
def multilot_auction():
    yield Auction(
        tender_id=lot_tender_data['data']['auctionID'],
        worker_defaults=yaml.load(open(worker_defaults_file_path)),
        auction_data=lot_tender_data,
        lot_id=lot_tender_data['data']['lots'][0]['id']
    )


@pytest.yield_fixture(scope="function")
def features_auction():
    yield Auction(
        tender_id=features_tender_data['data']['auctionID'],
        worker_defaults=yaml.load(open(worker_defaults_file_path)),
        auction_data=features_tender_data,
        lot_id=False
    )


@pytest.fixture(scope='function')
def db(request):
    server = couchdb.Server("http://" + worker_defaults['COUCH_DATABASE'].split('/')[2])
    name = worker_defaults['COUCH_DATABASE'].split('/')[3]

    def delete():
        del server[name]

    if name in server:
        delete()
    server.create(name)
    request.addfinalizer(delete)


class LogInterceptor(object):
    def __init__(self, logger):
        self.log_capture_string = StringIO()
        self.test_handler = logging.StreamHandler(self.log_capture_string)
        self.test_handler.setLevel(logging.INFO)
        logger.addHandler(self.test_handler)


@pytest.fixture(scope='function')
def logger():
    return LogInterceptor(LOGGER)


@pytest.fixture(scope='function')
def scheduler():
    return SCHEDULER


@pytest.fixture(scope='function')
def app():
    update_auctionPeriod(tender_data)
    logger = MagicMock()
    logger.name = 'some-logger'
    app_auction = Auction(
        tender_id=tender_data['data']['auctionID'],
        worker_defaults=yaml.load(open(worker_defaults_file_path)),
        auction_data=tender_data,
        lot_id=False
    )
    app_auction.bidders_data = tender_data['data']['bids']
    app_auction.db = MagicMock()
    app_auction.db.get.return_value = test_auction_document
    worker_app.config.update(app_auction.worker_defaults)
    worker_app.logger_name = logger.name
    worker_app._logger = logger
    worker_app.config['auction'] = app_auction
    worker_app.config['timezone'] = tz('Europe/Kiev')
    worker_app.config['SESSION_COOKIE_PATH'] = '/{}/{}'.format(
        'auctions', app_auction.auction_doc_id)
    worker_app.config['SESSION_COOKIE_NAME'] = 'auction_session'
    worker_app.oauth = MagicMock()
    worker_app.bids_form = BidsForm
    worker_app.form_handler = MagicMock()
    worker_app.form_handler.return_value = {'data': 'ok'}
    worker_app.remote_oauth = MagicMock()
    worker_app.remote_oauth.authorized_response.side_effect = [None, {
        u'access_token': u'aMALGpjnB1iyBwXJM6betfgT4usHqw',
        u'token_type': u'Bearer',
        u'expires_in': 86400,
        u'refresh_token': u'uoRKeSJl9UFjuMwOw6PikXuUVp7MjX',
        u'scope': u'email'
    }]
    worker_app.remote_oauth.authorize.return_value = \
        redirect('https://my.test.url')
    worker_app.logins_cache[(u'aMALGpjnB1iyBwXJM6betfgT4usHqw', '')] = {
        u'bidder_id': u'f7c8cd1d56624477af8dc3aa9c4b3ea3',
        u'expires':
            (datetime.datetime.now(tzlocal()) + timedelta(0, 600)).isoformat()
    }
    worker_app.auction_bidders = {
        u'f7c8cd1d56624477af8dc3aa9c4b3ea3': {
            'clients': {},
            'channels': {}
        }}
    yield worker_app.test_client()


def pytest_addoption(parser):
    parser.addoption("--worker", action="store_true", help="runs worker test", dest='worker')


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line("markers", "worker: mark test to run only if worker option is passed (--worker)")


def pytest_runtest_setup(item):
    worker_marker = item.get_marker("worker")
    if worker_marker is not None:
        # import pdb; pdb.set_trace()
        if not item.config.getoption("worker", False):
            pytest.skip("test requires worker option (--worker)")
