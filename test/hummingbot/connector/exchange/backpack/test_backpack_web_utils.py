import unittest

import hummingbot.connector.exchange.backpack.backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack import backpack_web_utils as web_utils


class BackpackWebUtilsTestCases(unittest.TestCase):

    def test_public_rest_url(self):
        path_url = "/TEST_PATH"
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))

    def test_private_rest_url(self):
        path_url = "/TEST_PATH"
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION + path_url
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))

    def test_wss_url(self):
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = CONSTANTS.WS_URL
        self.assertEqual(expected_url, web_utils.wss_url(CONSTANTS.WS_URL, domain))
