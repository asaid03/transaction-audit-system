"""Presentation cases built from existing findings, never a second detection engine."""

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from transaction_audit.rules import _invoice_match_key


PAYMENT_RELATIONSHIPS = {'possible_duplicate_payment', 'possible_split_payment'}
SEPARATE_FINDINGS = {'duplicate_transaction_id', 'vendor_concentration', 'balance_movement_mismatch'}
BANDS = {'low': 0, 'medium': 1, 'high': 2}


def _text(value: object) -> str:
    return '' if value is None else str(value).strip()


def _identity(record: dict) -> tuple[str, str]:
    transaction = record['transaction']
    vendor = _text(transaction.get('vendor_id'))
    name = ' '.join(_text(transaction.get('counterparty')).lower().split())
    return ('id', vendor) if vendor else ('name', name)


def _decimal(value: object) -> Decimal | None:
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def _invoice_summary(members: list[dict], tolerance: Decimal) -> dict | None:
    if len(members) < 2:
        return None
    transactions = [member['transaction'] for member in members]
    keys = {_invoice_match_key(t.get('invoice_reference')) for t in transactions}
    if None in keys or len(keys) != 1 or len({_identity(m) for m in members}) != 1:
        return None
    currencies = {_text(t.get('currency')) for t in transactions}
    if len(currencies) != 1 or '' in currencies:
        return None
    amounts = [_decimal(t.get('amount')) for t in transactions]
    invoices = [_decimal(t.get('invoice_amount')) for t in transactions]
    if any(value is None or value <= 0 for value in [*amounts, *invoices]):
        return None
    invoice = invoices[0]
    if any(abs(value - invoice) > tolerance for value in invoices):
        return None
    paid = sum(amounts, Decimal(0))
    difference = paid - invoice
    return {
        'currency': next(iter(currencies)), 'invoice': str(invoice), 'paid': str(paid),
        'excess': str(max(difference, Decimal(0)) if abs(difference) > tolerance else Decimal(0)),
        'remaining': str(max(-difference, Decimal(0)) if abs(difference) > tolerance else Decimal(0)),
    }


def _case(kind, members, findings, unresolved=(), context=(), tolerance=Decimal('0.01')):
    members = sorted(members, key=lambda item: item['row_number'])
    findings = sorted({f['finding_id']: f for f in findings}.values(), key=lambda f: str(f['finding_id']))
    representative = max(members, key=lambda m: (BANDS[m['audit_band']], m['audit_score'], -m['row_number'])) if members else None
    strongest = max(findings, key=lambda f: (BANDS.get(str(f.get('review_priority_band', 'low')).lower(), 0), f.get('review_priority_score', 0)), default=None)
    summary = _invoice_summary(members, tolerance) if kind == 'payments' and not unresolved else None
    title = strongest['finding'] if strongest else 'Unusual payment pattern'
    if summary and Decimal(summary['excess']) > 0:
        title = 'Possible invoice overpayment'
    elif any(f['technical_issue_type'] == 'possible_split_payment' for f in findings):
        title = 'Invoice paid in parts'
    totals = defaultdict(lambda: Decimal(0))
    invalid = set()
    for member in members:
        currency = _text(member['transaction'].get('currency')) or 'Unknown currency'
        value = _decimal(member['transaction'].get('amount'))
        if value is None:
            invalid.add(currency)
        else:
            totals[currency] += value
    # Unknown values must not appear as complete monetary totals.
    for currency in invalid:
        totals[currency] += Decimal(0)
    signature = kind + ':' + ','.join(m['id'] for m in members)
    if not members:
        signature += ':' + ','.join(f['finding_id'] for f in findings)
    model_member = max(members, key=lambda m: (bool(m['model']['flagged']), m['model']['score']), default=None)
    return {
        'id': 'case-' + sha256(signature.encode()).hexdigest()[:16],
        'kind': kind, 'title': title, 'row_number': members[0]['row_number'] if members else 0,
        'transaction': representative['transaction'] if representative else {},
        'audit_band': str(strongest.get('review_priority_band', 'low')).lower() if strongest else 'low',
        'audit_score': strongest.get('review_priority_score', 0) if strongest else 0,
        'audit_drivers': 'Highest existing finding priority; original scores retained per payment.',
        'findings': findings, 'members': members, 'context_members': list(context),
        'unresolved_links': sorted(set(unresolved)), 'invoice_summary': summary,
        'totals': [{'currency': c, 'amount': None if c in invalid else str(v)} for c, v in sorted(totals.items())],
        'model': model_member['model'] if model_member else {
            'score': 0, 'band': 'low', 'flagged': False, 'decision': None, 'context': 'No payment model output.'},
    }


def build_review_cases(transactions: list[dict], unlinked_findings: list[dict] | None = None,
                       invoice_tolerance: float = 0.01) -> list[dict]:
    """Combine compatible rule-linked rows; keep broad/context relationships separate.

    Source row IDs, not business transaction IDs, identify members. Reciprocal and
    transitive links are resolved once; missing/ambiguous references remain visible.
    """
    ordered = sorted(transactions, key=lambda r: r['row_number'])
    by_id = defaultdict(list)
    for record in ordered:
        transaction_id = _text(record['transaction'].get('transaction_id'))
        if transaction_id:
            by_id[transaction_id].append(record)
    parent = {r['id']: r['id'] for r in ordered}
    invoice_keys = {r['id']: {_invoice_match_key(r['transaction'].get('invoice_reference'))} - {None} for r in ordered}
    unresolved = defaultdict(set)

    def root(key):
        while key != parent[key]:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    for record in ordered:
        for finding in record['findings']:
            if finding['technical_issue_type'] not in PAYMENT_RELATIONSHIPS:
                continue
            if finding.get('related_dataset') not in ('', None, 'transactions'):
                unresolved[record['id']].add(_text(finding.get('related_record_id')))
                continue
            for peer_id in filter(None, map(str.strip, _text(finding.get('related_record_id')).split(','))):
                peers = by_id.get(peer_id, [])
                if len(peers) != 1:
                    unresolved[record['id']].add(peer_id)
                    continue
                peer = peers[0]
                left, right = root(record['id']), root(peer['id'])
                currency = _text(record['transaction'].get('currency'))
                compatible = (
                    bool(currency) and currency == _text(peer['transaction'].get('currency'))
                    and _identity(record) == _identity(peer)
                    and _identity(record)[1] != ''
                    and len(invoice_keys[left] | invoice_keys[right]) <= 1
                )
                if not compatible:
                    unresolved[record['id']].add(peer_id)
                elif left != right:
                    parent[right] = left
                    invoice_keys[left] |= invoice_keys[right]

    groups = defaultdict(list)
    for record in ordered:
        groups[root(record['id'])].append(record)
    result = []
    represented = set()
    tolerance = Decimal(str(invoice_tolerance))
    for members in groups.values():
        findings = [f for r in members for f in r['findings'] if f['technical_issue_type'] not in SEPARATE_FINDINGS]
        if findings:
            result.append(_case('payments', members, findings,
                                [p for r in members for p in unresolved[r['id']]], tolerance=tolerance))
            represented.update(r['id'] for r in members)

    separate = defaultdict(list)
    for record in ordered:
        for finding in record['findings']:
            kind = finding['technical_issue_type']
            if kind not in SEPARATE_FINDINGS:
                continue
            transaction = record['transaction']
            if kind == 'duplicate_transaction_id':
                key = (kind, _text(transaction.get('transaction_id')))
            elif kind == 'vendor_concentration':
                key = (kind, _identity(record), _text(transaction.get('currency')))
            else:
                key = (kind, record['id'])
            separate[key].append((record, finding))
    for key, entries in separate.items():
        members = list({r['id']: r for r, _ in entries}.values())
        context = []
        if key[0] == 'balance_movement_mismatch':
            ids = {_text(f.get('related_record_id')) for _, f in entries}
            context = [r for identifier in ids for r in by_id.get(identifier, []) if r['id'] not in {m['id'] for m in members}]
        result.append(_case(key[0], members, [f for _, f in entries], context=context, tolerance=tolerance))
        represented.update(m['id'] for m in members)

    for record in ordered:
        if record['id'] not in represented and record['model']['flagged']:
            result.append(_case('model', [record], []))
    for finding in unlinked_findings or []:
        result.append(_case('file', [], [finding]))
    cases_by_member = defaultdict(set)
    for case in result:
        for member in case['members']:
            cases_by_member[member['id']].add(case['id'])
    for case in result:
        related = set()
        for member in case['members']:
            related.update(cases_by_member[member['id']])
        case['related_case_ids'] = sorted(related - {case['id']})
    return sorted(result, key=lambda c: (-BANDS[c['audit_band']], -c['audit_score'], c['id']))
