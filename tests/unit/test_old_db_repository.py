from app.repositories.old_db import _normalize_phone_last10, _to_optional_scaled_float


def test_normalize_phone_last10_for_teyca_and_old_db_formats() -> None:
    assert _normalize_phone_last10("79039859055") == "9039859055"
    assert _normalize_phone_last10("9039859055") == "9039859055"
    assert _normalize_phone_last10("+7 (903) 985-90-55") == "9039859055"
    assert _normalize_phone_last10("abc") is None


def test_to_optional_scaled_float_divides_by_100_without_remainder() -> None:
    assert _to_optional_scaled_float(2330) == 23.0
    assert _to_optional_scaled_float("199") == 1.0
    assert _to_optional_scaled_float("100.9") == 1.0
    assert _to_optional_scaled_float(None) is None
