from openprocurement.auction.includeme import _register
from openprocurement.auction.interfaces import IAuctionsServer
from openprocurement.auction.worker.views import includeme


def belowThreshold(components):
    _register(components, 'belowThreshold')
    server = components.queryUtility(IAuctionsServer)
    includeme(server)


def aboveThresholdUA(components):
    _register(components, 'aboveThresholdUA')


def aboveThresholdEU(components):
    _register(components, 'aboveThresholdEU')


def competitiveDialogueEU(components):
    _register(components, 'competitiveDialogueEU.stage2')


def competitiveDialogueUA(components):
    _register(components, 'competitiveDialogueUA.stage2')


def aboveThresholdUAdefense(components):
    _register(components, 'aboveThresholdUA.defense')


def closeFrameworkAgreementUA(components):
    _register(components, 'closeFrameworkAgreementUA')


def closeFrameworkAgreementSelectionUA(components):
    _register(components, 'closeFrameworkAgreementSelectionUA')
