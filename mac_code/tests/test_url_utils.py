import pytest
from crawler import is_blocked, is_quality_url, passes_domain_path_allowlist, clean_url

def test_is_blocked():
    assert is_blocked("https://example.com/login") is True
    assert is_blocked("https://github.com/user/repo/pulls") is True
    assert is_blocked("https://example.com/image.png") is True
    assert is_blocked("https://example.com/about") is True  # /about is blocked
    assert is_blocked("https://hellointerview.com/learn/") is False

def test_passes_domain_path_allowlist():
    # Educative allows specific paths
    assert passes_domain_path_allowlist("https://www.educative.io/courses/abc") is True
    assert passes_domain_path_allowlist("https://www.educative.io/login") is False
    
    # A domain not in the allowlist should default to True
    assert passes_domain_path_allowlist("https://google.com/search") is True

def test_is_quality_url():
    # Valid tier1 URL
    is_ok, reason = is_quality_url("https://www.hellointerview.com/learn/system-design")
    assert is_ok is True
    
    # Blocked URL
    is_ok, reason = is_quality_url("https://bytebytego.com/premium")
    assert is_ok is False
    assert reason == "blocked"

    # Non-allowed domain
    is_ok, reason = is_quality_url("https://facebook.com")
    assert is_ok is False
    assert reason == "domain not allowed"

def test_clean_url():
    assert clean_url("https://example.com/page?utm_source=google") == "https://example.com/page"
    assert clean_url("https://example.com/page#fragment") == "https://example.com/page"
    assert clean_url("https://example.com/page?ref=123") == "https://example.com/page"