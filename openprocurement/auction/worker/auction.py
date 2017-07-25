import logging

from copy import deepcopy
from urlparse import urljoin
from datetime import datetime
from couchdb import Database, Session

from gevent import sleep
from gevent.event import Event
from gevent.lock import BoundedSemaphore

from yaml import safe_dump as yaml_dump
from requests import Session as RequestsSession
from dateutil.tz import tzlocal
from barbecue import cooking
from apscheduler.schedulers.gevent import GeventScheduler

from openprocurement.auction.worker.journal import (
    AUCTION_WORKER_SERVICE_AUCTION_RESCHEDULE,
    AUCTION_WORKER_SERVICE_AUCTION_NOT_FOUND,
    AUCTION_WORKER_SERVICE_AUCTION_STATUS_CANCELED,
    AUCTION_WORKER_SERVICE_AUCTION_CANCELED,
    AUCTION_WORKER_SERVICE_END_AUCTION,
    AUCTION_WORKER_SERVICE_START_AUCTION,
    AUCTION_WORKER_SERVICE_STOP_AUCTION_WORKER,
    AUCTION_WORKER_SERVICE_PREPARE_SERVER,
    AUCTION_WORKER_SERVICE_END_FIRST_PAUSE
)
from openprocurement.auction.worker.server import run_server
from openprocurement.auction.executor import AuctionsExecutor
from openprocurement.auction.worker.mixins import\
    DBServiceMixin, RequestIDServiceMixin, AuditServiceMixin,\
    DateTimeServiceMixin, BiddersServiceMixin, PostAuctionServiceMixin,\
    StagesServiceMixin, ROUNDS, TIMEZONE
from openprocurement.auction.worker.utils import \
    prepare_initial_bid_stage, prepare_results_stage

from openprocurement.auction.utils import\
    get_latest_bid_for_bidder, sorting_by_amount,\
    sorting_start_bids_by_amount, delete_mapping


LOGGER = logging.getLogger('Auction Worker')
SCHEDULER = GeventScheduler(job_defaults={"misfire_grace_time": 100},
                            executors={'default': AuctionsExecutor()},
                            logger=LOGGER)
SCHEDULER.timezone = TIMEZONE


class Auction(DBServiceMixin,
              RequestIDServiceMixin,
              AuditServiceMixin,
              BiddersServiceMixin,
              DateTimeServiceMixin,
              StagesServiceMixin,
              PostAuctionServiceMixin):
    """Auction Worker Class"""

    def __init__(self, tender_id,
                 worker_defaults={},
                 auction_data={},
                 lot_id=None):
        super(Auction, self).__init__()
        self.generate_request_id()
        self.tender_id = tender_id
        self.lot_id = lot_id
        if lot_id:
            self.auction_doc_id = tender_id + "_" + lot_id
        else:
            self.auction_doc_id = tender_id
        self.tender_url = urljoin(
            worker_defaults["resource_api_server"],
            '/api/{0}/{1}/{2}'.format(
                worker_defaults["resource_api_version"],
                worker_defaults["resource_name"],
                tender_id
            )
        )
        if auction_data:
            self.debug = True
            LOGGER.setLevel(logging.DEBUG)
            self._auction_data = auction_data
        else:
            self.debug = False
        self._end_auction_event = Event()
        self.bids_actions = BoundedSemaphore()
        self.session = RequestsSession()
        self.worker_defaults = worker_defaults
        if self.worker_defaults.get('with_document_service', False):
            self.session_ds = RequestsSession()
        self._bids_data = {}
        self.db = Database(str(self.worker_defaults["COUCH_DATABASE"]),
                           session=Session(retry_delays=range(10)))
        self.audit = {}
        self.retries = 10
        self.bidders_count = 0
        self.bidders_data = []
        self.bidders_features = {}
        self.bidders_coeficient = {}
        self.features = None
        self.mapping = {}
        self.rounds_stages = []

    def schedule_auction(self):
        self.generate_request_id()
        self.get_auction_document()
        if self.debug:
            LOGGER.info("Get _auction_data from auction_document")
            self._auction_data = self.auction_document.get('test_auction_data', {})
        self.get_auction_info()
        self.prepare_audit()
        self.prepare_auction_stages()
        self.save_auction_document()
        round_number = 0
        SCHEDULER.add_job(
            self.start_auction, 'date',
            kwargs={"switch_to_round": round_number},
            run_date=self.convert_datetime(
                self.auction_document['stages'][0]['start']
            ),
            name="Start of Auction",
            id="Start of Auction"
        )
        round_number += 1

        SCHEDULER.add_job(
            self.end_first_pause, 'date', kwargs={"switch_to_round": round_number},
            run_date=self.convert_datetime(
                self.auction_document['stages'][1]['start']
            ),
            name="End of Pause Stage: [0 -> 1]",
            id="End of Pause Stage: [0 -> 1]"
        )
        round_number += 1
        for index in xrange(2, len(self.auction_document['stages'])):
            if self.auction_document['stages'][index - 1]['type'] == 'bids':
                SCHEDULER.add_job(
                    self.end_bids_stage, 'date',
                    kwargs={"switch_to_round": round_number},
                    run_date=self.convert_datetime(
                        self.auction_document['stages'][index]['start']
                    ),
                    name="End of Bids Stage: [{} -> {}]".format(index - 1, index),
                    id="End of Bids Stage: [{} -> {}]".format(index - 1, index)
                )
            elif self.auction_document['stages'][index - 1]['type'] == 'pause':
                SCHEDULER.add_job(
                    self.next_stage, 'date',
                    kwargs={"switch_to_round": round_number},
                    run_date=self.convert_datetime(
                        self.auction_document['stages'][index]['start']
                    ),
                    name="End of Pause Stage: [{} -> {}]".format(index - 1, index),
                    id="End of Pause Stage: [{} -> {}]".format(index - 1, index)
                )
            round_number += 1
        LOGGER.info(
            "Prepare server ...",
            extra={"JOURNAL_REQUEST_ID": self.request_id,
                   "MESSAGE_ID": AUCTION_WORKER_SERVICE_PREPARE_SERVER}
        )
        self.server = run_server(self, self.convert_datetime(self.auction_document['stages'][-2]['start']), LOGGER)

    def wait_to_end(self):
        self._end_auction_event.wait()
        LOGGER.info("Stop auction worker",
                    extra={"JOURNAL_REQUEST_ID": self.request_id,
                           "MESSAGE_ID": AUCTION_WORKER_SERVICE_STOP_AUCTION_WORKER})

    def start_auction(self, switch_to_round=None):
        self.generate_request_id()
        self.audit['timeline']['auction_start']['time'] = datetime.now(tzlocal()).isoformat()
        LOGGER.info(
            '---------------- Start auction ----------------',
            extra={"JOURNAL_REQUEST_ID": self.request_id,
                   "MESSAGE_ID": AUCTION_WORKER_SERVICE_START_AUCTION}
        )
        self.get_auction_info()
        self.get_auction_document()
        # Initital Bids
        bids = deepcopy(self.bidders_data)
        self.auction_document["initial_bids"] = []
        bids_info = sorting_start_bids_by_amount(bids, features=self.features)
        for index, bid in enumerate(bids_info):
            amount = bid["value"]["amount"]
            audit_info = {
                "bidder": bid["id"],
                "date": bid["date"],
                "amount": amount
            }
            if self.features:
                amount_features = cooking(
                    amount,
                    self.features, self.bidders_features[bid["id"]]
                )
                coeficient = self.bidders_coeficient[bid["id"]]
                audit_info["amount_features"] = str(amount_features)
                audit_info["coeficient"] = str(coeficient)
            else:
                coeficient = None
                amount_features = None

            self.audit['timeline']['auction_start']['initial_bids'].append(
                audit_info
            )
            self.auction_document["initial_bids"].append(
                prepare_initial_bid_stage(
                    time=bid["date"] if "date" in bid else self.startDate,
                    bidder_id=bid["id"],
                    bidder_name=self.mapping[bid["id"]],
                    amount=amount,
                    coeficient=coeficient,
                    amount_features=amount_features
                )
            )
        if isinstance(switch_to_round, int):
            self.auction_document["current_stage"] = switch_to_round
        else:
            self.auction_document["current_stage"] = 0

        all_bids = deepcopy(self.auction_document["initial_bids"])
        minimal_bids = []
        for bid_info in self.bidders_data:
            minimal_bids.append(get_latest_bid_for_bidder(
                all_bids, str(bid_info['id'])
            ))

        minimal_bids = self.filter_bids_keys(sorting_by_amount(minimal_bids))
        self.update_future_bidding_orders(minimal_bids)
        self.save_auction_document()

    def end_first_pause(self, switch_to_round=None):
        self.generate_request_id()
        LOGGER.info(
            '---------------- End First Pause ----------------',
            extra={"JOURNAL_REQUEST_ID": self.request_id,
                   "MESSAGE_ID": AUCTION_WORKER_SERVICE_END_FIRST_PAUSE}
        )
        self.bids_actions.acquire()
        self.get_auction_document()

        if isinstance(switch_to_round, int):
            self.auction_document["current_stage"] = switch_to_round
        else:
            self.auction_document["current_stage"] += 1

        self.save_auction_document()
        self.bids_actions.release()

    def end_auction(self):
        LOGGER.info(
            '---------------- End auction ----------------',
            extra={"JOURNAL_REQUEST_ID": self.request_id,
                   "MESSAGE_ID": AUCTION_WORKER_SERVICE_END_AUCTION}
        )
        LOGGER.debug("Stop server", extra={"JOURNAL_REQUEST_ID": self.request_id})
        if self.server:
            self.server.stop()
        LOGGER.debug(
            "Clear mapping", extra={"JOURNAL_REQUEST_ID": self.request_id}
        )
        delete_mapping(self.worker_defaults,
                       self.auction_doc_id)

        start_stage, end_stage = self.get_round_stages(ROUNDS)
        minimal_bids = deepcopy(
            self.auction_document["stages"][start_stage:end_stage]
        )
        minimal_bids = self.filter_bids_keys(sorting_by_amount(minimal_bids))
        self.auction_document["results"] = []
        for item in minimal_bids:
            self.auction_document["results"].append(prepare_results_stage(**item))
        self.auction_document["current_stage"] = (len(self.auction_document["stages"]) - 1)
        LOGGER.debug(' '.join((
            'Document in end_stage: \n', yaml_dump(dict(self.auction_document))
        )), extra={"JOURNAL_REQUEST_ID": self.request_id})
        self.approve_audit_info_on_announcement()
        LOGGER.info('Audit data: \n {}'.format(yaml_dump(self.audit)), extra={"JOURNAL_REQUEST_ID": self.request_id})
        if self.debug:
            LOGGER.debug(
                'Debug: put_auction_data disabled !!!',
                extra={"JOURNAL_REQUEST_ID": self.request_id}
            )
            sleep(10)
            self.save_auction_document()
        else:
            if self.put_auction_data():
                self.save_auction_document()
        LOGGER.debug(
            "Fire 'stop auction worker' event",
            extra={"JOURNAL_REQUEST_ID": self.request_id}
        )

    def cancel_auction(self):
        self.generate_request_id()
        if self.get_auction_document():
            LOGGER.info("Auction {} canceled".format(self.auction_doc_id),
                        extra={'MESSAGE_ID': AUCTION_WORKER_SERVICE_AUCTION_CANCELED})
            self.auction_document["current_stage"] = -100
            self.auction_document["endDate"] = datetime.now(tzlocal()).isoformat()
            LOGGER.info("Change auction {} status to 'canceled'".format(self.auction_doc_id),
                        extra={'MESSAGE_ID': AUCTION_WORKER_SERVICE_AUCTION_STATUS_CANCELED})
            self.save_auction_document()
        else:
            LOGGER.info("Auction {} not found".format(self.auction_doc_id),
                        extra={'MESSAGE_ID': AUCTION_WORKER_SERVICE_AUCTION_NOT_FOUND})

    def reschedule_auction(self):
        self.generate_request_id()
        if self.get_auction_document():
            LOGGER.info("Auction {} has not started and will be rescheduled".format(self.auction_doc_id),
                        extra={'MESSAGE_ID': AUCTION_WORKER_SERVICE_AUCTION_RESCHEDULE})
            self.auction_document["current_stage"] = -101
            self.save_auction_document()
        else:
            LOGGER.info("Auction {} not found".format(self.auction_doc_id),
                        extra={'MESSAGE_ID': AUCTION_WORKER_SERVICE_AUCTION_NOT_FOUND})
