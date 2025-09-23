import asyncio
import unittest
from typing import Awaitable
from unittest.mock import AsyncMock, patch

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils import AsterPerpetualRESTPreProcessor
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class AsterPerpetualWebUtilsUnitTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.pre_processor = AsterPerpetualRESTPreProcessor()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_aster_perpetual_rest_pre_processor_non_post_request(self):
        """Test REST pre-processor for non-POST requests"""
        request: RESTRequest = RESTRequest(
            method=RESTMethod.GET,
            url="/TEST_URL",
        )

        result_request: RESTRequest = self.async_run_with_timeout(self.pre_processor.pre_process(request))

        self.assertIn("Content-Type", result_request.headers)
        self.assertEqual(result_request.headers["Content-Type"], "application/x-www-form-urlencoded")

    def test_aster_perpetual_rest_pre_processor_post_request(self):
        """Test REST pre-processor for POST requests"""
        request: RESTRequest = RESTRequest(
            method=RESTMethod.POST,
            url="/TEST_URL",
        )

        result_request: RESTRequest = self.async_run_with_timeout(self.pre_processor.pre_process(request))

        self.assertIn("Content-Type", result_request.headers)
        self.assertEqual(result_request.headers["Content-Type"], "application/json")

    def test_rest_pre_processor_with_existing_headers(self):
        """Test that pre-processor preserves existing headers"""
        request: RESTRequest = RESTRequest(
            method=RESTMethod.GET,
            url="/TEST_URL",
            headers={"Custom-Header": "custom-value"}
        )

        result_request: RESTRequest = self.async_run_with_timeout(self.pre_processor.pre_process(request))

        self.assertIn("Content-Type", result_request.headers)
        self.assertIn("Custom-Header", result_request.headers)
        self.assertEqual(result_request.headers["Custom-Header"], "custom-value")

    def test_public_rest_url_main_domain(self):
        """Test public REST URL generation for main domain"""
        path_url = "/TEST_PATH_URL"

        expected_url = f"{CONSTANTS.PERPETUAL_BASE_URL}{path_url}"
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url))

    def test_public_rest_url_testnet_domain(self):
        """Test public REST URL generation for testnet domain"""
        path_url = "/TEST_PATH_URL"

        expected_url = f"{CONSTANTS.TESTNET_BASE_URL}{path_url}"
        self.assertEqual(
            expected_url, web_utils.public_rest_url(path_url=path_url, domain="aster_perpetual_testnet")
        )

    def test_private_rest_url_main_domain(self):
        """Test private REST URL generation for main domain"""
        path_url = "/TEST_PATH_URL"

        expected_url = f"{CONSTANTS.PERPETUAL_BASE_URL}{path_url}"
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url))

    def test_private_rest_url_testnet_domain(self):
        """Test private REST URL generation for testnet domain"""
        path_url = "/TEST_PATH_URL"

        expected_url = f"{CONSTANTS.TESTNET_BASE_URL}{path_url}"
        self.assertEqual(
            expected_url, web_utils.private_rest_url(path_url=path_url, domain="aster_perpetual_testnet")
        )

    def test_wss_url_main_domain(self):
        """Test WebSocket URL generation for main domain"""
        endpoint = "TEST_SUBSCRIBE"

        expected_url = f"{CONSTANTS.PERPETUAL_WS_URL}{endpoint}"
        self.assertEqual(expected_url, web_utils.wss_url(endpoint=endpoint))

    def test_wss_url_testnet_domain(self):
        """Test WebSocket URL generation for testnet domain"""
        endpoint = "TEST_SUBSCRIBE"

        expected_url = f"{CONSTANTS.TESTNET_WS_URL}{endpoint}"
        self.assertEqual(expected_url, web_utils.wss_url(endpoint=endpoint, domain="aster_perpetual_testnet"))

    def test_build_api_factory(self):
        """Test API factory building with time synchronizer"""
        api_factory = web_utils.build_api_factory(
            time_synchronizer=TimeSynchronizer(),
            time_provider=lambda: None,
        )

        self.assertIsInstance(api_factory, WebAssistantsFactory)
        self.assertIsNone(api_factory._auth)
        self.assertEqual(2, len(api_factory._rest_pre_processors))

    def test_build_api_factory_without_time_synchronizer_pre_processor(self):
        """Test API factory building without time synchronizer"""
        throttler = web_utils.create_throttler()
        api_factory = web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler)

        self.assertIsInstance(api_factory, WebAssistantsFactory)
        self.assertEqual(1, len(api_factory._rest_pre_processors))
        self.assertIsInstance(api_factory._rest_pre_processors[0], AsterPerpetualRESTPreProcessor)

    def test_create_throttler(self):
        """Test throttler creation"""
        throttler = web_utils.create_throttler()
        self.assertIsNotNone(throttler)

    @patch("hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils.build_api_factory_without_time_synchronizer_pre_processor")
    def test_get_current_server_time(self, mock_build_factory):
        """Test server time retrieval"""
        # Mock the REST assistant and response
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value={"serverTime": 1234567890})

        mock_api_factory = AsyncMock()
        mock_api_factory.get_rest_assistant = AsyncMock(return_value=mock_rest_assistant)
        mock_build_factory.return_value = mock_api_factory

        # Test server time retrieval
        server_time = self.async_run_with_timeout(web_utils.get_current_server_time())

        self.assertEqual(server_time, 1234567890)
        mock_rest_assistant.execute_request.assert_called_once()

    @patch("hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils.build_api_factory_without_time_synchronizer_pre_processor")
    def test_get_current_server_time_alternative_response_format(self, mock_build_factory):
        """Test server time retrieval with alternative response format"""
        # Mock the REST assistant and response with alternative format
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value={"time": 9876543210})

        mock_api_factory = AsyncMock()
        mock_api_factory.get_rest_assistant = AsyncMock(return_value=mock_rest_assistant)
        mock_build_factory.return_value = mock_api_factory

        # Test server time retrieval
        server_time = self.async_run_with_timeout(web_utils.get_current_server_time())

        self.assertEqual(server_time, 9876543210)

    def test_is_exchange_information_valid_perpetual_trading(self):
        """Test exchange information validation for valid perpetual trading pair"""
        rule = {
            "contractType": "PERPETUAL",
            "status": "TRADING"
        }

        self.assertTrue(web_utils.is_exchange_information_valid(rule))

    def test_is_exchange_information_valid_not_perpetual(self):
        """Test exchange information validation for non-perpetual contract"""
        rule = {
            "contractType": "FUTURE",
            "status": "TRADING"
        }

        self.assertFalse(web_utils.is_exchange_information_valid(rule))

    def test_is_exchange_information_valid_not_trading(self):
        """Test exchange information validation for non-trading status"""
        rule = {
            "contractType": "PERPETUAL",
            "status": "BREAK"
        }

        self.assertFalse(web_utils.is_exchange_information_valid(rule))

    def test_is_exchange_information_valid_missing_fields(self):
        """Test exchange information validation with missing fields"""
        rule = {
            "contractType": "PERPETUAL"
            # Missing status field
        }

        self.assertFalse(web_utils.is_exchange_information_valid(rule))

    def test_is_exchange_information_valid_empty_rule(self):
        """Test exchange information validation with empty rule"""
        rule = {}

        self.assertFalse(web_utils.is_exchange_information_valid(rule))


if __name__ == "__main__":
    unittest.main()
