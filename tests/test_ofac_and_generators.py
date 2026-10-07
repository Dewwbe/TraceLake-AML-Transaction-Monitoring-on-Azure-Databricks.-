import random

from aml.generators.replay import write_microbatches
from aml.generators.synthetic import (
    SAML_COLUMNS,
    generate_customers,
    generate_fictional_sanctions,
    generate_transactions,
    perturb_name,
    seed_name_matches,
)
from aml.ingestion.ofac import ofac_to_canonical

# Fixture follows the layout ASSUMED in aml.ingestion.ofac (verify against OFAC's specification).
SDN = (
    '1001,"DOE, John",individual,TESTPROG,-0-,-0-,-0-,-0-,-0-,-0-,-0-,"DOB 1970"\n'
    '1002,"ACME TRADING LLC",-0-,TESTPROG,-0-,-0-,-0-,-0-,-0-,-0-,-0-,-0-\n'
)
ALT = '1001,5,aka,"JOHNNY DOE",-0-\n'


def test_ofac_conversion_handles_nulls_aliases_and_entities(tmp_path):
    sdn, alt = tmp_path / "sdn.csv", tmp_path / "alt.csv"
    sdn.write_text(SDN)
    alt.write_text(ALT)
    df = ofac_to_canonical(str(sdn), str(alt))
    assert len(df) == 3
    entity = df[df["name"] == "ACME TRADING LLC"].iloc[0]
    assert entity["entity_type"] == "entity" and entity["remarks"] is None
    alias = df[df["record_type"] == "alias"].iloc[0]
    assert alias["name"] == "JOHNNY DOE" and alias["ent_num"] == "1001" and alias["alt_num"] == "5"


def test_generators_are_deterministic_and_contain_expected_dirty_rows():
    a, _ = generate_transactions(n_accounts=60, n_normal=400, seed=3)
    b, _ = generate_transactions(n_accounts=60, n_normal=400, seed=3)
    assert a.equals(b) and list(a.columns) == SAML_COLUMNS
    assert (a["Amount"].astype(str).isin(["", "-500.0", "-500.00"])).sum() >= 2
    assert (a["Is_laundering"] == 1).sum() > 0
    assert set(a["Laundering_type"]) >= {"Normal", "Structuring", "Fan_out"}


def test_customers_and_seeded_ground_truth():
    _, acct = generate_transactions(n_accounts=80, n_normal=100, seed=3)
    customers, accounts = generate_customers(list(acct["account_id"]))
    assert accounts["account_id"].is_unique and set(accounts["customer_id"]) == set(customers["customer_id"])
    listed = generate_fictional_sanctions()
    names = listed.loc[listed["record_type"] == "primary", "name"].tolist()
    seeded, truth = seed_name_matches(customers, names)
    assert len(truth) == 18 and set(truth["perturbation"]) == {
        "exact", "reorder", "typo", "translit", "initial", "middle"}
    changed = seeded.set_index("customer_id").loc[truth["customer_id"], "full_name"]
    assert (changed.values != customers.set_index("customer_id").loc[truth["customer_id"], "full_name"].values).any()


def test_perturb_name_variants():
    rng = random.Random(1)
    assert perturb_name("Mohammed Haddad", "reorder", rng) == "Haddad, Mohammed"
    assert perturb_name("Mohammed Haddad", "initial", rng) == "M. Haddad"
    assert perturb_name("Mohammed Haddad", "translit", rng).startswith("Muhammad")
    assert perturb_name("Mohammed Haddad", "typo", rng) != "Mohammed Haddad"


def test_replay_writes_ordered_microbatches(tmp_path):
    df, _ = generate_transactions(n_accounts=30, n_normal=50, seed=1)
    files = write_microbatches(df, tmp_path, rows_per_file=20)
    assert len(files) == -(-len(df) // 20)
    assert files[0].name == "txn_000001.csv"
