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

    def i8(ns):
        import base64
        import struct
        encoded = base64.b64encode(struct.pack('<ff', 1.0, 2.0)).decode()
        ns.write(distance_metric='euclidean_squared', schema={'v': {'type': '[2]i8', 'ann': True}}, upsert_rows=[{'id': 1, 'v': [-128, 127]}, {'id': 2, 'v': encoded}, {'id': 3, 'v': [3, 4]}])
        result = [normalize(ns.query(rank_by=['v', 'ANN', [1, 2]], top_k=10, include_attributes=True))]
        result.append(normalize(ns.query(rank_by=['v', 'kNN', [1, 2]], filters=['id', 'Gt', 0], top_k=10, include_attributes=True, vector_encoding='base64')))
        for invalid in ([128, 0], [-129, 0], [0.5, 0]):
            try:
                ns.write(upsert_rows=[{'id': 4, 'v': invalid}])
                raise AssertionError('invalid i8 write succeeded')
            except turbopuffer.APIStatusError as exc:
                result.append({'status': exc.status_code})
            try:
                ns.query(rank_by=['v', 'ANN', invalid], top_k=1)
                raise AssertionError('invalid i8 query succeeded')
            except turbopuffer.APIStatusError as exc:
                result.append({'status': exc.status_code})
        return result

    scenarios['m7_i8'] = (i8, False)

    def highlight(ns):
        ns.write(schema={'s': {'type': 'string', 'full_text_search': True, 'fuzzy': True}, 'a': {'type': '[]string', 'full_text_search': True}}, upsert_rows=[
            {'id': 1, 's': '🐡 Red fox.\nThe fox runs.\n\nA bird flies.', 'a': ['A fox. Next bird.', 'More fox fox.']}, {'id': 2, 's': 'nothing'},
        ])
        result = []
        for options in ({}, {'include_offsets': 'utf-8'}, {'fragment_by': 'none', 'include_offsets': 'utf-16'}, {'fragment_by': 'word', 'include_offsets': 'codepoints'}, {'fragment_by': 'paragraph', 'include_offsets': 'utf-8'}, {'fragment_limit': 1}, {'rank_fragments_by': ['$fragment', 'BM25', 'bird'], 'include_offsets': 'utf-8'}):
            value = normalize(ns.query(rank_by=['s', 'BM25', 'fox'], top_k=10, compute_attributes={'h': ['Highlight', 's', options]}))
            result.append([r['h'] for r in value['rows']])
        for rank, attribute, options in (
            (['a', 'BM25', 'fox'], 'a', {'include_offsets': 'utf-8'}),
            (['s', 'Fuzzy', 'fox', {'max_edit_distance': [{'min_query_chars': 3, 'distance': 0}]}], 's', {'fragment_by': 'none', 'include_offsets': 'utf-8'}),
            (['id', 'asc'], 's', {'rank_fragments_by': ['$fragment', 'BM25', 'bird'], 'include_offsets': 'utf-8'}),
        ):
            value = normalize(ns.query(rank_by=rank, top_k=10, compute_attributes={'h': ['Highlight', attribute, options]}))
            result.append([r['h'] for r in value['rows']])
        return result

    scenarios['m7_highlight'] = (highlight, False)

    def branch(ns):
        from compat.cli import normalize
        source = ns.id + '-src'
        parent = ns._client.namespace(source)
        try:
            parent.write(upsert_rows=[{'id': 1, 's': 'first'}, {'id': 2, 's': 'second'}])
            value = ns.write(branch_from_namespace={'source_namespace': source})
            result = {'branch': normalize(value), 'after': normalize(ns.query(rank_by=['id', 'asc'], top_k=10, include_attributes=True))}
            metadata = normalize(ns.metadata())
            result['lineage'] = {'depth': metadata['branching']['depth'], 'parent_is_source': metadata['branching']['parent'] == source}
            parent.write(patch_rows=[{'id': 1, 's': 'source only'}])
            ns.write(patch_rows=[{'id': 2, 's': 'branch only'}])
            result['source'] = normalize(parent.query(rank_by=['id', 'asc'], top_k=10, include_attributes=True))
            parent.delete_all()
            result['independent'] = normalize(ns.query(rank_by=['id', 'asc'], top_k=10, include_attributes=True))
            try:
                ns.write(branch_from_namespace=ns.id)
                raise AssertionError('nonempty branch succeeded')
            except turbopuffer.APIStatusError as exc:
                result['nonempty_status'] = exc.status_code
            return result
        finally:
            try:
                parent.delete_all()
            except turbopuffer.NotFoundError:
                pass

    scenarios['m7_branch'] = (branch, False)

    def multivector(ns):
        try:
            ns.write(distance_metric='cosine_distance', schema={'tokens': {'type': '[][2]f32', 'ann': {'late_interaction': True}}}, upsert_rows=[
                {'id': 1, 'tokens': [[1, 0], [0, 1]]}, {'id': 2, 'tokens': [[1, 0]]}, {'id': 3, 'tokens': [[1, 1]]}, {'id': 4, 'tokens': []},
            ])
        except turbopuffer.APIStatusError as exc:
            message = str(exc.body.get('error', '')) if isinstance(exc.body, dict) else ''
            if exc.status_code == 400 and 'not enabled for this organization' in message:
                return {'availability': 'organization_gated', 'status': 400, 'error': message}
            raise
        value = normalize(ns.query(rank_by=['tokens', 'ANN', [[1, 0], [0, 1]]], top_k=10))
        import math
        assert [r['id'] for r in value['rows']] == [1, 3, 2]
        assert all(math.isclose(r['$dist'], d, abs_tol=1e-6) for r, d in zip(value['rows'], [0.0, 2 * (1 - 1 / math.sqrt(2)), 1.0]))
        return {'availability': 'supported'}

    scenarios['m7_multivector'] = (multivector, False)

    def unsupported_models(ns):
        ns.write(distance_metric='cosine_distance', upsert_rows=[{'id': 1, 'vector': [1.0, 0.0], 's': 'fox'}])
        result = []
        for params in (
            {'rank_by': ['vector', 'ANN', ['Embed', 'fox', {'model': 'blowfish-unsupported-model'}]], 'top_k': 1},
            {'rank_by': ['id', 'asc'], 'top_k': 1, 'rerank_by': {'model': 'blowfish-unsupported-model', 'attribute': 's', 'query': 'fox'}},
        ):
            try:
                ns.query(**params)
                raise AssertionError('unsupported model query succeeded')
            except turbopuffer.APIStatusError as exc:
                message = str(exc.body.get('error', '')) if isinstance(exc.body, dict) else ''
                result.append({'status': exc.status_code, 'message_present': bool(message)})
        return result

    scenarios['m7_unsupported_models'] = (unsupported_models, False)

    def schema_details(ns):
        ns.write(distance_metric='cosine_distance', schema={'s': {'type': '{}f16', 'sparse_knn': {'distance_metric': 'dot_product'}}, 'v': {'type': '[2]i8', 'ann': True}}, upsert_rows=[{'id': 1, 's': {'a': 1}, 'v': [1, 2]}])
        return {'schema': normalize(ns.schema()), 'metadata': normalize(ns.metadata())}

    scenarios['m7_schema'] = (schema_details, False)
