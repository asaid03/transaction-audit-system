from copy import deepcopy

from transaction_audit.cases import build_review_cases


def payment(row, transaction_id, amount=2500, invoice='INV-001', vendor='V1', currency='GBP'):
    return {
        'id': f'row-{row}', 'row_number': row,
        'transaction': {'transaction_id': transaction_id, 'counterparty': 'Supplier A',
                        'vendor_id': vendor, 'currency': currency, 'amount': amount,
                        'invoice_reference': invoice, 'invoice_amount': 2500,
                        'transaction_date': '2025-06-01', 'account_id': 'AP'},
        'audit_band': 'high', 'audit_score': 70, 'audit_drivers': 'Original driver',
        'findings': [],
        'model': {'score': 12, 'band': 'low', 'flagged': False, 'decision': 0.1, 'context': 'Original model context'},
    }


def finding(row, peer='', kind='possible_duplicate_payment'):
    return {'finding_id': f'{kind}-{row}', 'row_number': row, 'finding': kind,
            'technical_issue_type': kind, 'related_record_id': peer,
            'related_dataset': 'transactions', 'evidence': 'Original evidence',
            'why_it_matters': 'Review evidence', 'recommended_action': 'Check invoice',
            'review_priority_score': 70, 'review_priority_band': 'High'}


def test_reciprocal_links_form_one_case_and_preserve_inputs():
    a, b = payment(2, 'A'), payment(3, 'B', 2480)
    a['findings'] = [finding(2, 'B')]
    b['findings'] = [finding(3, 'A')]
    before = deepcopy([a, b])
    cases = build_review_cases([a, b])
    assert len(cases) == 1
    assert len(cases[0]['members']) == 2
    assert len(cases[0]['findings']) == 2
    assert cases[0]['invoice_summary']['paid'] == '4980'
    assert cases[0]['invoice_summary']['excess'] == '2480'
    assert [a, b] == before


def test_transitive_chain_and_input_order_are_stable():
    rows = [payment(2, 'A'), payment(3, 'B'), payment(4, 'C')]
    rows[0]['findings'] = [finding(2, 'B')]
    rows[1]['findings'] = [finding(3, 'C')]
    first = build_review_cases(rows)
    second = build_review_cases(list(reversed(rows)))
    assert len(first) == 1
    assert len(first[0]['members']) == 3
    assert first == second


def test_different_vendor_or_currency_or_invoice_does_not_merge():
    for overrides in ({'vendor': 'V2'}, {'currency': 'USD'}, {'invoice': 'OTHER'}):
        a, b = payment(2, 'A'), payment(3, 'B', **overrides)
        a['findings'] = [finding(2, 'B')]
        b['findings'] = [finding(3, 'A')]
        cases = build_review_cases([a, b])
        assert len(cases) == 2
        assert all(len(c['members']) == 1 for c in cases)
        assert all(c['unresolved_links'] for c in cases)


def test_shared_invoice_without_rule_relationship_does_not_merge():
    a, b = payment(2, 'A'), payment(3, 'B')
    a['findings'] = [finding(2, kind='large_amount')]
    b['findings'] = [finding(3, kind='large_amount')]
    assert len(build_review_cases([a, b])) == 2


def test_split_invoice_summary_and_original_variants():
    a, b = payment(2, 'A', 1000), payment(3, 'B', 1500, invoice='inv 1')
    a['findings'] = [finding(2, 'B', 'possible_split_payment')]
    b['findings'] = [finding(3, 'A', 'possible_split_payment')]
    case = build_review_cases([a, b])[0]
    assert case['invoice_summary']['excess'] == '0'
    assert case['invoice_summary']['remaining'] == '0'
    assert {p['transaction']['invoice_reference'] for p in case['members']} == {'INV-001', 'inv 1'}


def test_bad_invoice_values_do_not_invent_exposure():
    a, b = payment(2, 'A'), payment(3, 'B')
    a['findings'] = [finding(2, 'B')]
    for value in (None, -10, 'bad', 2600):
        b['transaction']['invoice_amount'] = value
        assert build_review_cases([a, b])[0]['invoice_summary'] is None


def test_duplicate_id_rows_are_not_lost_or_merged_by_currency():
    a, b = payment(2, 'SAME'), payment(3, 'SAME', currency='USD')
    a['findings'] = [finding(2, 'SAME', 'duplicate_transaction_id')]
    b['findings'] = [finding(3, 'SAME', 'duplicate_transaction_id')]
    case = build_review_cases([a, b])[0]
    assert len(case['members']) == 2
    assert len(case['totals']) == 2
    assert case['invoice_summary'] is None


def test_population_and_balance_links_do_not_join_unrelated_payments():
    a, b = payment(2, 'A'), payment(3, 'B')
    a['findings'] = [finding(2, 'Supplier A', 'vendor_concentration')]
    b['findings'] = [finding(3, 'A', 'balance_movement_mismatch')]
    cases = build_review_cases([a, b])
    assert len(cases) == 2
    assert {c['kind'] for c in cases} == {'vendor_concentration', 'balance_movement_mismatch'}
    assert all(len(c['members']) == 1 for c in cases)


def test_model_flag_does_not_duplicate_a_case_or_change_audit_priority():
    a = payment(2, 'A')
    a['findings'] = [finding(2, kind='missing_invoice_reference')]
    a['model'].update(score=100, band='high', flagged=True)
    case = build_review_cases([a])[0]
    assert case['audit_score'] == 70
    assert case['model']['score'] == 100
    assert len(build_review_cases([a])) == 1


def test_unresolved_and_file_findings_stay_visible():
    a = payment(2, 'A')
    a['findings'] = [finding(2, 'NOT-IN-FILE')]
    cases = build_review_cases([a], [finding(None, kind='missing_column')])
    assert len(cases) == 2
    assert next(c for c in cases if c['members'])['unresolved_links'] == ['NOT-IN-FILE']
    assert next(c for c in cases if not c['members'])['kind'] == 'file'


def test_ambiguous_peer_identifier_does_not_choose_first_source_row():
    a, b, c = payment(2, 'A'), payment(3, 'B'), payment(4, 'B')
    a['findings'] = [finding(2, 'B')]
    cases = build_review_cases([a, b, c])
    assert len(cases) == 1
    assert len(cases[0]['members']) == 1
    assert cases[0]['unresolved_links'] == ['B']


def test_empty_reference_cannot_bridge_different_invoices():
    a, b, c = payment(2, 'A'), payment(3, 'B', invoice=''), payment(4, 'C', invoice='OTHER')
    a['findings'] = [finding(2, 'B')]
    b['findings'] = [finding(3, 'C')]
    cases = build_review_cases([a, b, c])
    assert len(cases) == 1
    assert len(cases[0]['members']) == 2
    assert cases[0]['unresolved_links'] == ['C']


def test_shared_payment_does_not_merge_population_and_invoice_findings():
    a, b = payment(2, 'A'), payment(3, 'B')
    a['findings'] = [finding(2, 'B'), finding(2, 'Supplier A', 'vendor_concentration')]
    b['findings'] = [finding(3, 'A'), finding(3, kind='large_amount')]
    cases = build_review_cases([a, b])
    assert len(cases) == 2
    invoice = next(c for c in cases if c['kind'] == 'payments')
    assert len(invoice['findings']) == 3
    assert len(next(c for c in cases if c['kind'] == 'vendor_concentration')['members']) == 1
    assert len(invoice['related_case_ids']) == 1


def test_decimal_calculation_is_exact_and_missing_totals_stay_unavailable():
    a, b = payment(2, 'A', 0.1), payment(3, 'B', 0.2)
    a['transaction']['invoice_amount'] = b['transaction']['invoice_amount'] = 0.3
    a['findings'] = [finding(2, 'B', 'possible_split_payment')]
    case = build_review_cases([a, b])[0]
    assert case['invoice_summary']['paid'] == '0.3'
    assert case['invoice_summary']['remaining'] == '0'
    b['transaction']['amount'] = None
    case = build_review_cases([a, b])[0]
    assert case['invoice_summary'] is None
    assert case['totals'][0]['amount'] is None


def test_real_api_case_output_preserves_detection_and_scores():
    import pandas as pd
    from transaction_audit.api import analyse_data
    from transaction_audit.validation import validate_transactions

    frame = pd.DataFrame([payment(2, 'A')['transaction'], payment(3, 'B', 2480)['transaction']])
    expected = validate_transactions(frame).issues_dataframe()
    payload = analyse_data(frame, 'synthetic.csv', {column: column for column in frame})
    assert payload['finding_count'] == len(expected)
    assert len(payload['review_cases']) == 1
    case = payload['review_cases'][0]
    assert {m['id'] for m in case['members']} == {m['id'] for m in payload['transactions']}
    assert len(case['findings']) == len(expected)
    assert all(member in payload['transactions'] for member in case['members'])
