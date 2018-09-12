from openprocurement.auction.includeme import _register
from openprocurement.auction.interfaces import IAuctionsServer
from openprocurement.auction.worker.views import includeme


def english_components(components, procurement_method_types):
    for procurementMethodType in procurement_method_types:
        _register(components, procurementMethodType)


def english_routes(components):
    server = components.queryUtility(IAuctionsServer)
    includeme(server)
