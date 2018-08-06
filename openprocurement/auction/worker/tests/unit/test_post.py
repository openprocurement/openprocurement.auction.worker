from copy import deepcopy
from requests import Session

from openprocurement.auction.worker_core.mixins import AuditServiceMixin
from openprocurement.auction.worker.tests.data.data import (
    tender_data, test_organization, lot_tender_data
)


def test_post_announce(auction, db, logger, mocker):
    test_bids = deepcopy(tender_data['data']['bids'])

    for bid in test_bids:
        bid['tenderers'] = [test_organization]
    mock_session_request = mocker.patch.object(Session, 'request', autospec=True)
    mock_session_request.return_value.json.return_value = {
        'data': {
            'bids': test_bids
        }
    }
    auction.prepare_auction_document()
    auction.post_announce()

    log_strings = logger.log_capture_string.getvalue().split('\n')

    """
    ['Bidders count: 2',
     'Saved auction document UA-11111 with rev 1-50b6586b0769fa09fd95039ac0a73284',
     'Get auction document UA-11111 with rev 1-50b6586b0769fa09fd95039ac0a73284',
     'Saved auction document UA-11111 with rev 2-20ae6bf47e139e274492a09dc61c204d',
     '']
    """

    assert 'Get auction document UA-11111 with rev 1-' in log_strings[2]
    assert 'Saved auction document UA-11111 with rev 2-' in log_strings[3]


def test_post_announce_multilot(multilot_auction, db, logger, mocker):
    test_bids = deepcopy(lot_tender_data['data']['bids'])

    for bid in test_bids:
        bid['tenderers'] = [test_organization]
    mock_session_request = mocker.patch.object(Session, 'request', autospec=True)
    mock_session_request.return_value.json.return_value = {
        'data': {
            'bids': test_bids
        }
    }
    multilot_auction.prepare_auction_document()
    multilot_auction.post_announce()

    log_strings = logger.log_capture_string.getvalue().split('\n')

    """
     ['Saved auction document UA-11111_2222222222222222 with rev 1-2802d34ff0ca367e06dd87492071620b',
     'Get auction document UA-11111_2222222222222222 with rev 1-2802d34ff0ca367e06dd87492071620b',
     'Saved auction document UA-11111_2222222222222222 with rev 2-665cb61789c7925140421559cdbe20c0',
     '']

    """
    assert 'Get auction document UA-11111_2222222222222222 with rev 1-' in log_strings[1]
    assert 'Saved auction document UA-11111_2222222222222222 with rev 2-' in log_strings[2]


def test_put_auction_data(auction, db, mocker, logger):

    test_bids = deepcopy(tender_data['data']['bids'])

    for bid in test_bids:
        bid['tenderers'] = [test_organization]

    mock_session_request = mocker.patch.object(Session, 'request', autospec=True)
    mock_session_request.return_value.json.side_effect = [
        {'data': {'id': 'UA-11111'}},
        {'data': {'bids': test_bids}},
    ]

    auction.prepare_auction_document()
    auction.get_auction_info()
    auction.prepare_auction_stages_fast_forward()
    auction.prepare_audit()

    response = auction.put_auction_data()
    assert response is True # actually returns True object
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert "Auctions results not approved" not in log_strings

    mock_session_request.return_value.json.side_effect = [
        {'data': {'id': 'UA-11111'}},
        False,
    ]

    response = auction.put_auction_data()
    assert response is None  # method does not return anything
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert "Auctions results not approved" in log_strings


def test_put_auction_data_with_document_service(auction, db, mocker, logger):
    auction.worker_defaults['with_document_service'] = True
    test_bids = deepcopy(tender_data['data']['bids'])

    for bid in test_bids:
        bid['tenderers'] = [test_organization]

    mock_session_request = mocker.patch.object(Session, 'request', autospec=True)
    mock_session_request.return_value.json.side_effect = [
        {'data': {'bids': test_bids}},
    ]


    auction.prepare_auction_document()
    auction.get_auction_info()
    auction.prepare_auction_stages_fast_forward()
    auction.prepare_audit()

    mock_upload_audit_file_with_document_service = mocker.patch.object(AuditServiceMixin, 'upload_audit_file_with_document_service', autospec=True)
    mock_upload_audit_file_with_document_service.return_value = 'UA-11111'

    response = auction.put_auction_data()

    assert mock_upload_audit_file_with_document_service.called is True
    assert mock_upload_audit_file_with_document_service.call_count == 2
    assert mock_upload_audit_file_with_document_service.call_args_list[0][0] == (auction, )
    assert mock_upload_audit_file_with_document_service.call_args_list[1][0] == (auction, 'UA-11111')


def test_put_auction_data_multilot(multilot_auction, db, mocker, logger):

    test_bids = deepcopy(lot_tender_data['data']['bids'])

    for bid in test_bids:
        bid['tenderers'] = [test_organization]

    mock_session_request = mocker.patch.object(Session, 'request', autospec=True)
    mock_session_request.return_value.json.side_effect = [
        {'data': {'id': 'UA-11111'}},
        {'data': {'bids': test_bids}},
    ]

    multilot_auction.prepare_auction_document()
    multilot_auction.get_auction_info()
    multilot_auction.prepare_auction_stages_fast_forward()
    multilot_auction.prepare_audit()

    response = multilot_auction.put_auction_data()

    assert response is None  # method does not return anything
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert "Auctions results not approved" not in log_strings

    mock_session_request.return_value.json.side_effect = [
        {'data': {'id': 'UA-11111'}},
        False,
    ]

    response = multilot_auction.put_auction_data()
    assert response is None  # method does not return anything
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert "Auctions results not approved" in log_strings
