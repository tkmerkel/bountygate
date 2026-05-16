from arbitrage_executor.account_scraper import parse_balance_text


def test_parse_balance_text_handles_dollars_and_commas():
    assert parse_balance_text("$2,847.23") == 2847.23


def test_parse_balance_text_handles_negative_pnl():
    assert parse_balance_text("-$142.50") == -142.50


def test_parse_balance_text_returns_none_on_garbage():
    assert parse_balance_text("--") is None
    assert parse_balance_text("") is None


def test_parse_balance_text_handles_plain_number():
    assert parse_balance_text("47.13") == 47.13


def test_parse_balance_text_strips_label_prefix():
    # "Balance: $123.45" — money-regex pulls out the amount only.
    assert parse_balance_text("Balance: $123.45") == 123.45
