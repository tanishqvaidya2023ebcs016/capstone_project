import pytest
from crawler import ScoringEngine

def test_source_authority_github():
    engine = ScoringEngine()
    # Should match the repo authority
    assert engine.get_source_authority("https://github.com/donnemartin/system-design-primer") == 1.0
    # Should match domain authority fallback
    assert engine.get_source_authority("https://github.com/random-user/repo") == 0.75
    assert engine.get_source_authority("https://bytebytego.com/course") == 1.0

def test_relevance_and_score(mocker):
    # Mock the HTML to simulate a page with keywords
    html = """
    <html><head><title>System Design Interview</title></head>
    <body>
        <article>
            <p>This is a system design guide covering distributed systems, microservices, and scalability.</p>
            <p>We talk about caching, load balancing, and database sharding.</p>
        </article>
    </body>
    </html>
    """
    engine = ScoringEngine()
    # We don't need to mock the date, we just want to check relevance calculation
    total_weight, norm, found, count = engine.calculate_relevance(html)
    
    # Check keyword hits: distributed systems, microservices, scalability, etc.
    assert count >= 4
    assert norm > 0.5  # Normalized relevance should be high