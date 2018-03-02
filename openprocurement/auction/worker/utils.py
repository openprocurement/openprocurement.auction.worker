# -*- coding: utf-8 -*-
import logging
import socket
from urlparse import urljoin, urlparse

from couchdb import Server, Session
from requests import request, Session as RequestsSession

from openprocurement.auction.utils import make_request

logging.addLevelName(25, 'CHECK')


def check(self, msg, exc=None, *args, **kwargs):
    self.log(25, msg)
    if exc:
        self.error(exc, exc_info=True)


logging.Logger.check = check

LOGGER = logging.getLogger('Auction Worker')


def prepare_initial_bid_stage(bidder_name="", bidder_id="", time="",
                              amount_features="", coeficient="", amount=""):
    stage = dict(bidder_id=bidder_id, time=str(time))
    stage["label"] = dict(
        en="Bidder #{}".format(bidder_name),
        uk="Учасник №{}".format(bidder_name),
        ru="Участник №{}".format(bidder_name)
    )

    stage['amount'] = amount if amount else 0
    if amount_features is not None and amount_features != "":
        stage['amount_features'] = str(amount_features)
    if coeficient:
        stage['coeficient'] = str(coeficient)
    return stage


prepare_results_stage = prepare_initial_bid_stage  # Looks identical


def prepare_bids_stage(exist_stage_params, params={}):
    exist_stage_params.update(params)
    stage = dict(type="bids", bidder_id=exist_stage_params['bidder_id'],
                 start=str(exist_stage_params['start']), time=str(exist_stage_params['time']))
    stage["amount"] = exist_stage_params['amount'] if exist_stage_params['amount'] else 0
    if 'amount_features' in exist_stage_params:
        stage["amount_features"] = exist_stage_params['amount_features']
    if 'coeficient' in exist_stage_params:
        stage["coeficient"] = exist_stage_params['coeficient']

    if exist_stage_params['bidder_name']:
        stage["label"] = {
            "en": "Bidder #{}".format(exist_stage_params['bidder_name']),
            "ru": "Участник №{}".format(exist_stage_params['bidder_name']),
            "uk": "Учасник №{}".format(exist_stage_params['bidder_name'])
        }
    else:
        stage["label"] = {
            "en": "",
            "ru": "",
            "uk": ""
        }
    return stage


def prepare_service_stage(**kwargs):
    pause = {
        "type": "pause",
        "start": ""
    }
    pause.update(kwargs)
    return pause


def init_services(auction):
    exceptions = []

    # Checking API availability
    result = ('ok', None)
    api_url = "{resource_api_server}/api/{resource_api_version}/health"
    try:
        if auction.debug:
            response = True
        else:
            response = make_request(url=api_url.format(**auction.worker_defaults),
                                    method="get", retry_count=5)
        if not response:
            raise Exception("Auction DS can't be reached")
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    else:
        auction.tender_url = urljoin(
            auction.worker_defaults["resource_api_server"],
            "/api/{0}/{1}/{2}".format(
                auction.worker_defaults["resource_api_version"],
                auction.worker_defaults["resource_name"],
                auction.tender_id
            )
        )
    LOGGER.check('{} - {}'.format("Document Service", result[0]), result[1])

    # Checking DS availability
    result = ('ok', None)
    if auction.worker_defaults.get("with_document_service", False):
        ds_config = auction.worker_defaults.get("DOCUMENT_SERVICE")
        try:
            resp = request("GET", ds_config.get("url"), timeout=5)
            if not resp or resp.status_code != 200:
                raise Exception("Auction DS can't be reached")
        except Exception as e:
            exceptions.append(e)
            result = ('failed', e)
        else:
            auction.session_ds = RequestsSession()
    LOGGER.check('{} - {}'.format("API", result[0]), result[1])

    # Checking CouchDB availability
    result = ('ok', None)
    server, db = auction.worker_defaults.get("COUCH_DATABASE").rsplit('/', 1)
    try:
        server = Server(server, session=Session(retry_delays=range(10)))
        database = server[db] if db in server else server.create(db)
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    else:
        auction.db = database
    LOGGER.check('{} - {}'.format("CouchDB", result[0]), result[1])

    if exceptions:
        raise exceptions[0]
