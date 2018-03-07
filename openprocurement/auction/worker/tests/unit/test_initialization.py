from copy import deepcopy
from uuid import uuid4

import pytest
from requests import exceptions

from openprocurement.auction.worker.auction import Auction
from openprocurement.auction.worker.mixins import Server


def test_init_services(worker_config, logger, mocker):

    test_config = deepcopy(worker_config)
    test_config['with_document_service'] = True
    test_config['DOCUMENT_SERVICE']['url'] = "http://1.2.3.4:6543/"
    test_config['resource_api_server'] = "http://1.2.3.4:6543/"

    tender_id = uuid4().hex  # random tender id

    mock_make_request = mocker.MagicMock()
    mock_make_request.side_effect = Exception("API can't be reached")
    mocker.patch('openprocurement.auction.worker.mixins.make_request', mock_make_request)

    with pytest.raises(Exception) as e:
        Auction(tender_id, test_config)
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert e.value.message == "API can't be reached"

    assert log_strings.count("API can't be reached") == 1
    assert "ConnectTimeout" in log_strings[-2]

    with pytest.raises(exceptions.RequestException) as e:
        Auction(tender_id, test_config, {'test_auction_data': True})
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert "ConnectTimeout" in str(e.value.message)

    assert log_strings.count("API can't be reached") == 1
    assert "ConnectTimeout" in log_strings[-2]

    test_config['with_document_service'] = False
    auction = Auction(tender_id, test_config, {'test_auction_data': True})

    assert auction.__getattribute__("tender_url") is not None
    assert auction.__getattribute__("db") is not None

    mock_server_connect = mocker.patch.object(Server, "__init__", autospec=True)
    mock_server_connect.side_effect = Exception("Connection refused")

    with pytest.raises(Exception) as e:
        Auction(tender_id, test_config, {'test_auction_data': True})
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert e.value.message == "Connection refused"

    assert log_strings.count("API can't be reached") == 1
    assert log_strings.count("Connection refused") == 1
