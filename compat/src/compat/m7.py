"""Remaining-feature scenarios; every namespace is reclaimed by the harness."""
import turbopuffer


def register(scenarios):
    """Register M7 oracle and implementation checks."""
    from compat.cli import normalize

    def fuzzy(ns):
        ns.write(schema={'s': {'type': 'string', 'fuzzy': True}, 'a': {'type': '[]string', 'fuzzy': True}}, upsert_rows=[
            {'id': 1, 's': 'prefix turbopuffer suffix', 'a': ['turbo', 'puffer']},
            {'id': 2, 's': 'Turbopufer', 'a': ['a turbopuffer']},
            {'id': 3, 's': 'túrbopuffer'}, {'id': 4},
        ])
        params = {'max_edit_distance': [{'min_query_chars': 3, 'distance': 0}, {'min_query_chars': 6, 'distance': 1}], 'case_sensitive': False}
        filters = [['s', 'Fuzzy', 'turbopufer', params], ['s', 'Fuzzy', 'turbopuffer', params], ['s', 'Fuzzy', 'tu', params], ['a', 'Fuzzy', 'turbopuffer', params], ['Not', ['s', 'Fuzzy', 'turbopuffer', params]]]
        result = [normalize(ns.query(rank_by=['id', 'asc'], top_k=10, filters=f)) for f in filters]
        ns.write(patch_rows=[{'id': 1, 's': 'unrelated'}], deletes=[2])
        result.append(normalize(ns.query(rank_by=['s', 'Fuzzy', 'turbopuffer', params], top_k=10)))
        return result

    def fuzzy_invalid(ns):
        ns.write(schema={'s': {'type': 'string', 'fuzzy': True}}, upsert_rows=[{'id': 1, 's': 'turbopuffer'}])
        result = []
        for params in (None, {}, {'max_edit_distance': [{'min_query_chars': 3, 'distance': 1}]}):
            f = ['s', 'Fuzzy', 'turbopuffer'] + ([] if params is None else [params])
            try:
                ns.query(rank_by=['id', 'asc'], top_k=10, filters=f)
                raise AssertionError('invalid fuzzy query succeeded')
            except turbopuffer.APIStatusError as exc:
                result.append({'status': exc.status_code})
        return result

    scenarios['m7_fuzzy'] = (fuzzy, False)
    scenarios['m7_fuzzy_invalid'] = (fuzzy_invalid, False)
