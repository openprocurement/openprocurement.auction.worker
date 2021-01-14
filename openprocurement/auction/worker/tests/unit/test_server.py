import json
from flask import session
from datetime import datetime, timedelta
from dateutil.tz import tzlocal
from mock import MagicMock, patch
from openprocurement.auction.worker.server import _LoggerStream
from flask_oauthlib.client import OAuthException


def test_logger_stream_write():
    logger = MagicMock()
    ls = _LoggerStream(logger)
    test_message = 'This is useful message.'
    extra = {'MESSAGE_ID': 'useful_id'}
    ls.write(test_message)
    logger.info.assert_called_with(test_message)
    ls.write(test_message, extra=extra)
    logger.info.assert_called_with(test_message, extra=extra)


def test_server_login(app):
    headers = {
        'X-Forwarded-Path':
            'http://localhost:8090/auctions/11111111111111111111111111111111'
            '/authorized?code=HVRviZDxswGzM8AYN3rz0qMLrh6rhY'
    }
    res = app.post('/login', headers=headers)
    assert res.status == '405 METHOD NOT ALLOWED'
    assert res.status_code == 405

    res = app.get('/login')
    assert res.status == '401 UNAUTHORIZED'
    assert res.status_code == 401

    res = app.get('/login?bidder_id=5675acc9232942e8940a034994ad883e&'
                  'hash=bd4a790aac32b73e853c26424b032e5a29143d1f')
    assert res.status == '302 FOUND'
    assert res.status_code == 302
    assert res.location == 'https://my.test.url'
    with app.application.test_request_context():
        session['login_bidder_id'] = u'5675acc9232942e8940a034994ad883e'
        session['login_hash'] = u'bd4a790aac32b73e853c26424b032e5a29143d1f'
        session['login_callback'] = 'http://localhost/authorized'
        log_message = 'Session: {}'.format(repr(session))
        app.application.logger.debug.assert_called_with(log_message)

    res = app.get('/login?bidder_id=5675acc9232942e8940a034994ad883e&'
                  'hash=bd4a790aac32b73e853c26424b032e5a29143d1f',
                  headers=headers)
    assert res.status == '302 FOUND'
    assert res.status_code == 302
    assert res.location == 'https://my.test.url'
    with app.application.test_request_context():
        session[u'login_bidder_id'] = u'5675acc9232942e8940a034994ad883e'
        session[u'login_hash'] = u'bd4a790aac32b73e853c26424b032e5a29143d1f'
        session[u'login_callback'] = u'http://localhost:8090/auctions/' \
            '11111111111111111111111111111111/authorized'
        log_message = 'Session: {}'.format(repr(session))
        app.application.logger.debug.assert_called_with(log_message)

    res = app.get('/login?bidder_id=5675acc9232942e8940a034994ad883e&'
                  'hash=bd4a790aac32b73e853c26424b032e5a29143d1f&'
                  'return_url=https://my.secret.url/')
    assert res.status == '302 FOUND'
    assert res.status_code == 302
    assert res.location == 'https://my.test.url'
    with app.application.test_request_context():
        session['return_url'] = u'https://my.secret.url/'
        session['login_bidder_id'] = u'5675acc9232942e8940a034994ad883e'
        session['login_hash'] = u'bd4a790aac32b73e853c26424b032e5a29143d1f'
        session['login_callback'] = 'http://localhost/authorized'

    app.application.remote_oauth.authorize.side_effect = OAuthException("Invalid response")
    res = app.get('/login?bidder_id=5675acc9232942e8940a034994ad883e&'
                  'hash=bd4a790aac32b73e853c26424b032e5a29143d1f',
                  headers=headers)
    assert res.status == "503 SERVICE UNAVAILABLE"


def test_server_authorized(app):
    headers = {
        'X-Forwarded-Path':
            'http://localhost:8090/auctions/11111111111111111111111111111111'
            '/authorized?code=HVRviZDxswGzM8AYN3rz0qMLrh6rhY'
    }

    res = app.post('/authorized', headers=headers)
    assert res.status == '405 METHOD NOT ALLOWED'
    assert res.status_code == 405

    res = app.get('/authorized', headers=headers)
    assert res.status_code == 403
    assert res.status == '403 FORBIDDEN'

    res = app.get('/authorized?error=access_denied')
    assert res.status_code == 403
    assert res.status == '403 FORBIDDEN'

    res = app.get('/authorized', headers=headers)
    assert res.status_code == 302
    assert res.status == '302 FOUND'
    assert res.location == \
        'http://localhost:8090/auctions/11111111111111111111111111111111'
    auctions_loggedin = False
    auction_session = False
    path = False
    for h in res.headers:
        if h[1].startswith('auctions_loggedin=1'):
            auctions_loggedin = True
            if h[1].index('Path=/auctions/UA-11111'):
                path = True
        if h[1].startswith('auction_session='):
            auction_session = True
    assert auction_session is True
    assert auctions_loggedin is True
    assert path is True

    app.application.remote_oauth.authorized_response.side_effect = OAuthException("Invalid response")
    res = app.get('/authorized', headers=headers)
    assert res.status == "503 SERVICE UNAVAILABLE"


def test_server_relogin(app):
    headers = {
        'X-Forwarded-Path':
            'http://localhost:8090/auctions/11111111111111111111111111111111'
            '/authorized?code=HVRviZDxswGzM8AYN3rz0qMLrh6rhY'
    }

    res = app.post('/relogin', headers=headers)
    assert res.status == '405 METHOD NOT ALLOWED'
    assert res.status_code == 405

    res = app.get('/relogin', headers=headers)
    assert res.status_code == 302
    assert res.status == '302 FOUND'
    assert res.location == \
        'http://localhost:8090/auctions/11111111111111111111111111111111'
    s = {
        'login_callback': 'https://some.url/',
        'login_bidder_id': 'some_id',
        'login_hash': 'some_cache',
        'amount': 100
    }
    with patch('openprocurement.auction.worker.server.session', s):
        res = app.get('/relogin?amount=100', headers=headers)
    assert res.status_code == 302
    assert res.status == '302 FOUND'
    assert res.location == 'https://my.test.url'

    app.application.remote_oauth.authorize.side_effect = OAuthException("Invalid response")
    with patch('openprocurement.auction.worker.server.session', s):
        res = app.get('/relogin', headers=headers)
    assert res.status == "503 SERVICE UNAVAILABLE"


def test_server_check_authorization(app):

    res = app.get('/check_authorization')
    assert res.status == '405 METHOD NOT ALLOWED'
    assert res.status_code == 405

    s = {
        'remote_oauth': (u'aMALGpjnB1iyBwXJM6betfgT4usHqw', ''),
        'client_id': 'b3a000cdd006b4176cc9fafb46be0273'
    }

    res = app.post('/check_authorization')
    assert res.status == '401 UNAUTHORIZED'
    assert res.status_code == 401

    with patch('openprocurement.auction.worker.server.session', s):
        res = app.post('/check_authorization')
    assert res.status == '200 OK'
    assert res.status_code == 200
    assert json.loads(res.data)['status'] == 'ok'

    with patch('openprocurement.auction.worker.server.session', s):
        app.application.logins_cache[
            (u'aMALGpjnB1iyBwXJM6betfgT4usHqw', '')
        ]['expires'] = (
            (datetime.now(tzlocal()) - timedelta(0, 600)).isoformat()
        )
        res = app.post('/check_authorization')
    assert res.status == '401 UNAUTHORIZED'
    assert res.status_code == 401
    app.application.logger.info.assert_called_with(
        'Grant will end in a short time. Activate re-login functionality',
        extra={}
    )
    s['remote_oauth'] = 'invalid'

    with patch('openprocurement.auction.worker.server.session', s):
        res = app.post('/check_authorization')
    assert res.status == '401 UNAUTHORIZED'
    assert res.status_code == 401
    app.application.logger.warning.assert_called_with(
        "Client_id {} didn't passed check_authorization".format(
            s['client_id']), extra={})


def test_server_logout(app):
    s = {
        'remote_oauth': (u'aMALGpjnB1iyBwXJM6betfgT4usHqw', ''),
        'client_id': 'b3a000cdd006b4176cc9fafb46be0273'
    }
    headers = {
        'X-Forwarded-Path':
            'http://localhost:8090/auctions/11111111111111111111111111111111'
            '/authorized?code=HVRviZDxswGzM8AYN3rz0qMLrh6rhY'
    }
    with patch('openprocurement.auction.worker.server.session', s):
        res = app.get('/logout', headers=headers)
    assert res.status_code == 302
    assert res.status == '302 FOUND'
    assert res.location == \
        'http://localhost:8090/auctions/11111111111111111111111111111111'


def test_server_postbid(app):

    res = app.get('/postbid')
    assert res.status == '405 METHOD NOT ALLOWED'
    assert res.status_code == 405

    s = {
        'remote_oauth': (u'aMALGpjnB1iyBwXJM6betfgT4usHqw', ''),
        'client_id': 'b3a000cdd006b4176cc9fafb46be0273'
    }
    with patch('openprocurement.auction.worker.server.session', s):
        res = app.post(
            '/postbid',
            data=json.dumps(
                {'bidder_id': u'f7c8cd1d56624477af8dc3aa9c4b3ea3'}
            ),
            headers={'Content-Type': 'application/json'}
        )
    assert res.status == '200 OK'
    assert res.status_code == 200
    assert json.loads(res.data)['data'] == 'ok'

    with patch('openprocurement.auction.worker.server.session', s):
        res = app.post(
            '/postbid',
            data=json.dumps(
                {'bidder_id': u'5675acc9232942e8940a034994666666'}
            ),
            headers={'Content-Type': 'application/json'}
        )
    mess_str = \
        'Client with client id: b3a000cdd006b4176cc9fafb46be0273 and ' \
        'bidder_id 5675acc9232942e8940a034994666666 wants post bid but ' \
        'response status from Oauth'
    app.application.logger.warning.assert_called_with(mess_str)
    assert res.status == '401 UNAUTHORIZED'
    assert res.status_code == 401


def test_server_kickclient(app):
    s = {
        'remote_oauth': (u'aMALGpjnB1iyBwXJM6betfgT4usHqw', ''),
        'client_id': 'b3a000cdd006b4176cc9fafb46be0273'
    }
    data = {
        'client_id': s['client_id'],
        'bidder_id': u'f7c8cd1d56624477af8dc3aa9c4b3ea3'
    }
    headers = {'Content-Type': 'application/json'}

    res = app.get('/kickclient')
    assert res.status == '405 METHOD NOT ALLOWED'
    assert res.status_code == 405

    res = app.post('/kickclient', data=json.dumps(data), headers=headers)
    assert res.status == '401 UNAUTHORIZED'
    assert res.status_code == 401

    with patch('openprocurement.auction.worker.server.session', s):
        res = app.post('/kickclient', data=json.dumps(data), headers=headers)
    assert res.status == '200 OK'
    assert res.status_code == 200
    assert json.loads(res.data)['status'] == 'ok'
