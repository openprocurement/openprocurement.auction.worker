import pytest
import json
from mock import MagicMock, patch
from wtforms.validators import ValidationError
from openprocurement.auction.worker.tests.data.data import (
    test_auction_document
)
from openprocurement.auction.worker.forms import (
    validate_bid_value, BidsForm, form_handler, validate_bid_change_on_bidding
)
from copy import deepcopy


def test_validate_bid_value():
    field = MagicMock()

    field.data = -0.0
    with pytest.raises(ValidationError) as e:
        validate_bid_value(None, field)
    assert e.value.message == u'Too low value'

    field.data = -0.99999999999
    with pytest.raises(ValidationError) as e:
        validate_bid_value(None, field)
    assert e.value.message == u'Too low value'

    field.data = -1.0000001
    with pytest.raises(ValidationError) as e:
        validate_bid_value(None, field)
    assert e.value.message == u'Too low value'

    field.data = 0.000001
    validate_bid_value(None, field)


def test_bid_change_float_precision_issue(auction):
    """
    checks the fix of the issue CBD-1723
    """
    form = BidsForm()
    form.auction = auction
    form.document = deepcopy(test_auction_document)
    form.document['minimalStep']['amount'] = 72.03
    form.document['stages'][-1]['amount'] = 14395.71

    field = form.bid
    field.data = 14323.68

    try:
        validate_bid_change_on_bidding(form, form.bid)
    except ValidationError:
        pytest.fail("Unexpected ValidationError: {} - {} = {}".format(
            form.document['stages'][-1]['amount'],
            form.document['minimalStep']['amount'],
            field.data))


def test_bids_form(auction, features_auction):
    form_errors = {
        'bid': [u'Bid amount is required'],
        'bidder_id': [u'No bidder id']
    }

    form_data = {}

    form = BidsForm().from_json({})
    assert form.validate() is False
    assert form.errors == form_errors

    form = BidsForm().from_json({'bidder_id': 'bidder_id'})
    form.document = test_auction_document
    assert form.validate() is False
    assert form.errors == {'bid': [u'Bid amount is required'],
                           'bidder_id': [u'Not valid bidder']}

    form = BidsForm().from_json({'bidder_id': 123})
    form.document = test_auction_document
    assert form.validate() is False
    assert form.errors == {'bid': [u'Bid amount is required'],
                           'bidder_id': [u'Not valid bidder']}

    form_data['bidder_id'] = u'f7c8cd1d56624477af8dc3aa9c4b3ea3'
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    assert form.validate() is False
    assert form.errors == {'bid': [u'Bid amount is required']}

    form_data['bid'] = 26000000
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is False
    assert form.errors == {'bid': [u'Too high value']}

    form_data['bid'] = -1
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is True
    assert form.errors == {}
    assert form.data == form_data

    form_data['bid'] = -1.0
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is True
    assert form.errors == {}
    assert form.data == form_data

    form_data['bid'] = '-1'
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is True
    assert form.errors == {}
    assert form.data['bid'] == -1.0

    form_data['bid'] = '-1.0'
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is True
    assert form.errors == {}
    assert form.data['bid'] == -1.0

    form_data['bid'] = '26000000'
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is False
    assert form.errors == {'bid': [u'Too high value']}

    form_data['bid'] = 'one'
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is False
    assert form.errors == {'bid': [u'Not a valid float value',
                                   u'Too low value']}

    form_data['bid'] = 120
    form = BidsForm().from_json(form_data)
    form.document = test_auction_document
    form.auction = auction
    assert form.validate() is True
    assert form.errors == {}
    assert form.data == form_data

    # features
    form_data['bid'] = 12
    form = BidsForm().from_json(form_data)
    form.auction = features_auction
    form.document = test_auction_document
    form.document['stages'][2]['amount_features'] = \
        form.document['stages'][2]['amount']
    form.auction.features = features_auction._auction_data['data']['features']
    form.auction.bidders_coeficient = {'f7c8cd1d56624477af8dc3aa9c4b3ea3': 0.1}
    assert form.validate() is True
    assert form.errors == {}
    assert form.data == form_data

    form_data['bid'] = '12'
    form = BidsForm().from_json(form_data)
    form.auction = features_auction
    form.document = test_auction_document
    form.document['stages'][2]['amount_features'] = \
        form.document['stages'][2]['amount']
    form.auction.features = features_auction._auction_data['data']['features']
    form.auction.bidders_coeficient = {'f7c8cd1d56624477af8dc3aa9c4b3ea3': 0.1}
    assert form.validate() is True
    assert form.errors == {}
    assert form.data['bid'] == float(form_data['bid'])

    form_data['bid'] = -1
    form = BidsForm().from_json(form_data)
    form.auction = features_auction
    form.document = test_auction_document
    form.document['stages'][2]['amount_features'] = \
        form.document['stages'][2]['amount']
    form.auction.features = features_auction._auction_data['data']['features']
    form.auction.bidders_coeficient = {'f7c8cd1d56624477af8dc3aa9c4b3ea3': 0.1}
    assert form.validate() is True
    assert form.errors == {}
    assert form.data == form_data

    form_data['bid'] = 123456
    form = BidsForm().from_json(form_data)
    form.auction = features_auction
    form.document = test_auction_document
    form.document['stages'][2]['amount_features'] = \
        form.document['stages'][2]['amount']
    form.auction.features = features_auction._auction_data['data']['features']
    form.auction.bidders_coeficient = {'f7c8cd1d56624477af8dc3aa9c4b3ea3': 0.1}
    assert form.validate() is False
    assert form.errors == {'bid': [u'Too high value']}

    form_data['bid'] = 123456
    form = BidsForm().from_json(form_data)
    form.auction = features_auction
    form.document = test_auction_document
    form.document['stages'][test_auction_document['current_stage']]['type'] =\
        'not_bids'
    form.document['stages'][2]['amount_features'] = \
        form.document['stages'][2]['amount']
    form.auction.features = features_auction._auction_data['data']['features']
    form.auction.bidders_coeficient = {'f7c8cd1d56624477af8dc3aa9c4b3ea3': 0.1}
    assert form.validate() is False
    assert form.errors == {'bid': [u'Stage not for bidding']}
    form.document['stages'][test_auction_document['current_stage']]['type'] =\
        'bids'


def test_form_handler(app):
    app.application.form_handler = form_handler
    headers = {'Content-Type': 'application/json'}
    s = {
        'remote_oauth': (u'aMALGpjnB1iyBwXJM6betfgT4usHqw', ''),
        'client_id': 'b3a000cdd006b4176cc9fafb46be0273'
    }
    data = {'bidder_id': 'f7c8cd1d56624477af8dc3aa9c4b3ea3', 'bid': -1}
    with patch('openprocurement.auction.worker.server.session', s), \
            patch('openprocurement.auction.worker.forms.session', s):
        res = app.post('/postbid', data=json.dumps(data), headers=headers)
    assert res.status == '200 OK'
    assert res.status_code == 200
    res_data = {'data': data}
    res_data['status'] = 'ok'
    assert json.loads(res.data) == res_data

    data['bid'] = 123
    with patch('openprocurement.auction.worker.server.session', s), \
            patch('openprocurement.auction.worker.forms.session', s):
        res = app.post('/postbid', data=json.dumps(data), headers=headers)
    assert res.status == '200 OK'
    assert res.status_code == 200
    res_data = {'data': data}
    res_data['status'] = 'ok'
    assert json.loads(res.data) == res_data

    data['bid'] = 26000000
    with patch('openprocurement.auction.worker.server.session', s), \
            patch('openprocurement.auction.worker.forms.session', s):
        res = app.post('/postbid', data=json.dumps(data), headers=headers)
    assert res.status == '200 OK'
    assert res.status_code == 200
    assert json.loads(res.data) == {
        u'status': u'failed',
        u'errors': {
            u'bid': [u'Too high value']
        }
    }
