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

    def patterns(ns):
        ns.write(schema={'s': {'type': 'string', 'regex': True, 'glob': True}, 'scan': {'type': 'string', 'filterable': True}}, upsert_rows=[
            {'id': 1, 's': 'turbopuffer.txt', 'scan': 'turbopuffer.txt'}, {'id': 2, 's': 'TURBOPUFFER.rs'}, {'id': 3, 's': 'turbofish.txt'}, {'id': 4, 's': 'ſtring.TXT'}, {'id': 5},
        ])
        filters = [
            ['s', 'Regex', r'turbo(puffer|fish)\.txt$'], ['s', 'Regex', '(turbo)?fish|PUFFER'], ['s', 'Regex', '(?i)turbopuffer'],
            ['s', 'Regex', '^.*$'], ['s', 'Glob', '*puffer.*'], ['s', 'NotGlob', '*puffer.*'], ['s', 'IGlob', '*PUFFER.*'],
            ['s', 'NotIGlob', '*PUFFER.*'], ['s', 'IGlob', 'STRING.*'], ['scan', 'Glob', '*puffer.*'], ['Not', ['s', 'Regex', 'fish']],
        ]
        return [normalize(ns.query(rank_by=['id', 'asc'], top_k=10, filters=f)) for f in filters]

    scenarios['m7_regex_glob'] = (patterns, False)

    def sparse(ns):
        ns.write(schema={'s': {'type': '{}f16', 'sparse_knn': {'distance_metric': 'dot_product'}}}, upsert_rows=[
            {'id': 1, 's': {'a': 0.3333, 'b': 2.0}}, {'id': 2, 's': {'b': 1.0}}, {'id': 3, 's': {}}, {'id': 4},
        ])
        ranks = [['s', 'SparseKNN', {'a': 0.3333, 'b': 1.0}], ['Sum', [['s', 'SparseKNN', {'a': 0.3333}], ['s', 'SparseKNN', {'b': 1.0}]]], ['Saturate', ['s', 'SparseKNN', {'b': 1.0}], {'midpoint': 2}], ['Max', [['s', 'SparseKNN', {'b': 1.0}], 0]], ['s', 'SparseKNN', {}]]
        result = [normalize(ns.query(rank_by=r, top_k=10, include_attributes=True)) for r in ranks]
        ns.write(patch_rows=[{'id': 2, 's': {'b': 10.0}}], deletes=[1])
        result.append(normalize(ns.query(rank_by=ranks[0], top_k=10, include_attributes=True)))
        return result

    scenarios['m7_sparse'] = (sparse, False)
