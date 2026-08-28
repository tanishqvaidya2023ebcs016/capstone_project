import pytest
import grpc
from crawler import ResilientQueueStub
import crawler_pb2

def test_failover_switching(mocker):
    # Mock 2 queue servers
    mock_logger = mocker.Mock()
    stub = ResilientQueueStub(["server1:50051", "server2:50051"], mock_logger)

    # Mock the gRPC stubs to simulate failures
    m1 = mocker.Mock()
    m2 = mocker.Mock()
    stub._stubs = [m1, m2]
    stub._active = 0

    # Create a request object
    request = crawler_pb2.GetNextURLRequest(crawler_id="test")

    # First 3 attempts on server1 should fail (FAILOVER_THRESHOLD is 3)
    m1.GetNextURL.side_effect = grpc.RpcError("Connection refused")
    
    for _ in range(3):
        with pytest.raises(grpc.RpcError):
            stub.GetNextURL(request)

    # The stub should have switched to server2 (active = 1)
    assert stub._active == 1
    assert stub._fail_cnt == 0 # Reset after failover
    mock_logger.warning.assert_called_once_with(
        "⚡ Queue server failover: server1:50051 → server2:50051 (after 3 consecutive failures)"
    )

    # Now server2 works, it should not go back to server1
    m2.GetNextURL.return_value = crawler_pb2.GetNextURLResponse(url="http://example.com", queue_empty=False)
    resp = stub.GetNextURL(request)
    assert resp.url == "http://example.com"
    assert stub._active == 1