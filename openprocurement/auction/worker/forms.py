# from da
from flask import request, session, current_app as app

from wtforms import Form, FloatField, StringField
from wtforms.validators import InputRequired, ValidationError, StopValidation
from fractions import Fraction
from datetime import datetime
from pytz import timezone
import wtforms_json
import decimal
from openprocurement.auction.utils import prepare_extra_journal_fields

wtforms_json.init()


def validate_bid_value(form, field):
    """
    Bid must be greater then 0
    """
    if field.data <= 0.0 and field.data != -1:
        raise ValidationError(u'Too low value')


def validate_bid_change_on_bidding(form, field):
    """
    Bid must be lower then previous bidder bid amount minus minimalStep amount
    """
    stage_id = form.document['current_stage']
    if form.auction.features:
        minimal_bid = form.document['stages'][stage_id]['amount_features']
        minimal = Fraction(minimal_bid) * form.auction.bidders_coeficient[form.data['bidder_id']]
        minimal -= Fraction(form.document['minimalStep']['amount'])
        if field.data > minimal:
            raise ValidationError(u'Too high value')
    else:
        minimal_bid = form.document['stages'][stage_id]['amount']
        with decimal.localcontext() as ctx:
            ctx.prec = 2
            max_allowed_bid = decimal.Decimal(minimal_bid) - decimal.Decimal(form.document['minimalStep']['amount'])
            if field.data is not None and decimal.Decimal(field.data).normalize() > max_allowed_bid:
                raise ValidationError(u'Too high value')


def validate_bidder_id_on_bidding(form, field):
    stage_id = form.document['current_stage']
    if field.data != form.document['stages'][stage_id]['bidder_id']:
        raise StopValidation(u'Not valid bidder')


class BidsForm(Form):
    bidder_id = StringField('bidder_id',
                            [InputRequired(message=u'No bidder id'), ])

    bid = FloatField('bid', [InputRequired(message=u'Bid amount is required'),
                             validate_bid_value])

    def validate_bid(self, field):
        stage_id = self.document['current_stage']
        if self.document['stages'][stage_id]['type'] == 'bids':
            validate_bid_change_on_bidding(self, field)
        else:
            raise ValidationError(u'Stage not for bidding')

    def validate_bidder_id(self, field):
        stage_id = self.document['current_stage']
        if self.document['stages'][stage_id]['type'] == 'bids':
            validate_bidder_id_on_bidding(self, field)


def form_handler():
    auction = app.config['auction']
    with auction.bids_actions:
        form = app.bids_form.from_json(request.json)
        form.auction = auction
        form.document = auction.db.get(auction.auction_doc_id)
        current_time = datetime.now(timezone('Europe/Kiev'))
        if form.validate():
            # write data
            auction.add_bid(form.document['current_stage'],
                            {'amount': form.data['bid'],
                             'bidder_id': form.data['bidder_id'],
                             'time': current_time.isoformat()})
            if form.data['bid'] == -1.0:
                app.logger.info("Bidder {} with client_id {} canceled bids in stage {} in {}".format(
                    form.data['bidder_id'], session['client_id'],
                    form.document['current_stage'], current_time.isoformat()
                ), extra=prepare_extra_journal_fields(request.headers))
            else:
                app.logger.info("Bidder {} with client_id {} placed bid {} in {}".format(
                    form.data['bidder_id'], session['client_id'],
                    form.data['bid'], current_time.isoformat()
                ), extra=prepare_extra_journal_fields(request.headers))
            return {'status': 'ok', 'data': form.data}
        else:
            app.logger.info("Bidder {} with client_id {} wants place bid {} in {} with errors {}".format(
                request.json.get('bidder_id', 'None'), session['client_id'],
                request.json.get('bid', 'None'), current_time.isoformat(),
                repr(form.errors)
            ), extra=prepare_extra_journal_fields(request.headers))
            return {'status': 'failed', 'errors': form.errors}
