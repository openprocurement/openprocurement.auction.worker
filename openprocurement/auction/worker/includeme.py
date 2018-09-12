from openprocurement.auction.includeme import _register


def english_components(components, procurement_method_types):
    for procurementMethodType in procurement_method_types:
        _register(components, procurementMethodType)
