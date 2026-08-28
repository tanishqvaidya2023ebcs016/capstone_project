import pytest
from web_server import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_index_route(client):
    response = client.get('/')
    assert response.status_code == 200
    assert b'<title>CrawlerX</title>' in response.data

def test_api_stats_route(client, mocker):
    # Mock the connection and cursor chain
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.fetchone.return_value = {'total': 150, 'avg_score': 6.5}  # real dict
    mock_conn.execute.return_value = mock_cursor

    mocker.patch('web_server.get_db', return_value=mock_conn)

    response = client.get('/api/stats')
    assert response.status_code == 200
    assert response.json['total_links'] == 150
    assert response.json['avg_score'] == 6.5

def test_api_links_route(client, mocker):
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_rows = [
        {'url': 'http://test.com', 'score': 7.5, 'title': 'Test', 'extracted_at': '2026-07-19'}
    ]
    mock_cursor.fetchall.return_value = mock_rows
    mock_conn.execute.return_value = mock_cursor

    mocker.patch('web_server.get_db', return_value=mock_conn)

    response = client.get('/api/links')
    assert response.status_code == 200
    assert len(response.json) == 1
    assert response.json[0]['score'] == 7.5

def test_api_tts_not_configured(client):
    # The TTS endpoint is no longer present, so we expect 404
    response = client.get('/api/tts?url=test')
    assert response.status_code == 404