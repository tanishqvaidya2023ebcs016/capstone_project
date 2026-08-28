import pytest
import time
import grpc
from grpc_server import QueueServiceServicer
from grpc_file_server import FileServiceServicer
import crawler_pb2

def test_queue_server_get_next_url(mocker):
    # Mock Redis client
    mock_redis = mocker.Mock()
    # Simulate Redis returning a URL via SPOP
    mock_redis.eval.return_value = "http://example.com"
    
    servicer = QueueServiceServicer(mock_redis)
    request = crawler_pb2.GetNextURLRequest(crawler_id="test")
    context = mocker.Mock()
    
    response = servicer.GetNextURL(request, context)
    assert response.url == "http://example.com"
    assert response.queue_empty is False

def test_queue_server_add_urls(mocker):
    mock_redis = mocker.Mock()
    # Simulate Redis returning 2 new URLs added
    mock_redis.eval.return_value = 2
    
    servicer = QueueServiceServicer(mock_redis)
    request = crawler_pb2.AddURLsRequest(
        urls=["https://test1.com", "https://test2.com"],
        crawler_id="test"
    )
    context = mocker.Mock()
    
    response = servicer.AddURLs(request, context)
    assert response.added_count == 2

def test_file_server_store_link(mocker):
    # Mock built-in open
    mock_open = mocker.mock_open()
    mocker.patch('builtins.open', mock_open)

    servicer = FileServiceServicer("/tmp/test_links.txt", "/tmp/raw.log")

    request = crawler_pb2.StoreLinkRequest(
        url="https://test.com",
        crawler_id="test",
        score=5.0,
        domain="test.com",
        timestamp=int(time.time())
    )
    context = mocker.Mock()

    response = servicer.StoreLink(request, context)
    assert response.success is True

    # 1. Check that the ranked file was opened in 'w' mode (overwrite) at least once
    ranked_write_calls = [
        call for call in mock_open.call_args_list
        if call[0] == ('/tmp/test_links.txt', 'w') and call[1].get('encoding') == 'utf-8'
    ]
    assert len(ranked_write_calls) >= 1

    # 2. Check that the raw log file was opened in 'a' mode (append) at least once
    raw_append_calls = [
        call for call in mock_open.call_args_list
        if call[0] == ('/tmp/raw.log', 'a') and call[1].get('encoding') == 'utf-8'
    ]
    assert len(raw_append_calls) >= 1

    # 3. Verify that the link appears in the write calls (ranked file)
    write_calls = mock_open().write.call_args_list
    ranked_content_found = any(
        'https://test.com' in str(call) and '[SCORE: 5.00]' in str(call)
        for call in write_calls
    )
    assert ranked_content_found

    # 4. Verify that the link appears in the raw log write calls
    raw_content_found = any(
        '[test]' in str(call) and '[SCORE: 5.00]' in str(call)
        for call in write_calls
    )
    assert raw_content_found