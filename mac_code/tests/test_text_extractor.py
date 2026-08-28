import pytest
import requests
import sqlite3
from text_extractor_worker import TextExtractorWorker
import os

def test_extractor_skips_binary_content(mocker):
    worker = TextExtractorWorker()
    
    # Mock the session.get request
    mock_get = mocker.patch.object(worker.session, 'get')
    
    # Simulate a binary content-type (image/png) response
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'image/png'}
    mock_get.return_value = mock_response

    # Run fetch_url
    result = worker.fetch_url("http://example.com/image.png", 5.0)
    
    assert result['status'] == 'skip'
    assert result['error'] == 'Binary/Non-text content-type: image/png'

def test_extractor_handles_unicode_decode_error(mocker):
    worker = TextExtractorWorker()
    mock_get = mocker.patch.object(worker.session, 'get')
    
    # Simulate bad binary data with misleading text header (must be text/html to reach decode step)
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'text/html'}  # changed from 'text/plain'
    mock_response.content = b'\xff\xfe\x12\x34'  # Invalid UTF-8 bytes
    mock_get.return_value = mock_response

    result = worker.fetch_url("http://example.com/random_binary", 5.0)
    
    assert result['status'] == 'skip'
    assert result['error'] == 'Binary content (UTF-8 decode failed)'

def test_extractor_parses_grpc_links_file(tmpdir, mocker):
    # Create a mock grpc_links.txt file
    grpc_file = tmpdir.join("grpc_links.txt")
    grpc_file.write("[2026-07-19T13:11:46] [crawler-mac] [SCORE: 7.54] https://test.com\n")
    
    # Override environment variable to use the mock file
    mocker.patch('text_extractor_worker.GRPC_LINKS_FILE', str(grpc_file))
    worker = TextExtractorWorker()
    
    # Mock the DB check so it thinks the URL is unprocessed
    mocker.patch.object(worker, 'is_url_processed', return_value=False)

    # Run the parser
    results = list(worker._iter_links_file(str(grpc_file), "grpc_links.txt"))
    
    assert len(results) == 1
    assert results[0][0] == "https://test.com"
    assert results[0][1] == 7.54