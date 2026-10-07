import pytest

from aml.transform.names import band_for, normalise_name, score_names


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  Dr. José  O'Brien-Smith ", "jose o brien smith"),
        ("HADDAD, Khalid", "haddad khalid"),
        ("Mr Ahmed   Al-Mansoori", "ahmed al mansoori"),
        (None, ""),
        ("", ""),
    ],
)
def test_normalise_name(raw, expected):
    assert normalise_name(raw) == expected


def test_normalise_keeps_non_latin_letters():
    assert normalise_name("محمد علي") == "محمد علي"


def test_score_identical_and_reordered_names_are_high():
    assert score_names("khalid haddad", "khalid haddad") == 100.0
    assert score_names("haddad khalid", "khalid haddad") >= 95


def test_score_typo_is_above_review_threshold():
    assert score_names("khalid haddad", "khalid hadadd") >= 90


def test_score_unrelated_names_are_low():
    assert score_names("khalid haddad", "maria silva") < 70


def test_score_empty_is_zero():
    assert score_names("", "x") == 0.0


def test_band_for():
    assert band_for(95, 80, 92) == "STRONG"
    assert band_for(85, 80, 92) == "REVIEW"
    assert band_for(50, 80, 92) is None
