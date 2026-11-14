"""Additional end-to-end scenarios, including raw HTTP and recall checks."""
import gzip
import json
import random

import httpx

from compat.cli import ROWS, normalize


def run(c, name, kind):
    ns = c.namespace(name)
    base = str(c.base_url).rstrip('/')
    headers = {"Authorization": f"Bearer {c.api_key}"}
    url = f"{base}/v2/namespaces/{name}"
    def request(method, path, **kw):
        response = httpx.request(method, base + path, headers=headers | kw.pop('headers', {}), timeout=60, **kw)
        try:
            body = response.json()
        except ValueError:
            body = None
        return response.status_code, body
    def write(target, **kwargs):
        return normalize(target.write(**kwargs))
    def setup():
        write(ns, upsert_rows=ROWS, distance_metric="cosine_distance", schema={"text": {"type": "string", "full_text_search": True}})
    if kind == 'types_roundtrip':
        values = {'s': 'hello', 'i': -4, 'u': 4, 'f': 1.5, 'b': True, 'uuid': 'a8098c1a-f86e-11da-bd1a-00112444be1e', 'dt': '2024-01-02T03:04:05Z', 'bytes': 'aGVsbG8='}
        types = {'s': 'string', 'i': 'int', 'u': 'uint', 'f': 'float', 'b': 'bool', 'uuid': 'uuid', 'dt': 'datetime', 'bytes': 'bytes'}
        for key in list(values):
            if key != 'bytes':
                values[key + '_array'] = [values[key]]
                types[key + '_array'] = '[]' + types[key]
        values['half'] = [0.25, 0.5, 0.75]
        types['half'] = '[3]f16'
        write(ns, upsert_rows=[{'id': 1, **values}], schema={k: {'type': v, **({'ann': True} if k == 'half' else {})} for k, v in types.items()}, distance_metric='cosine_distance')
        return normalize(ns.query(rank_by=['id', 'asc'], top_k=10, include_attributes=True))
    if kind == 'ann_recall':
        rng = random.Random(42)
        vectors = [[rng.uniform(-1, 1) for _ in range(16)] for _ in range(2020)]
        write(ns, upsert_rows=[{'id': i, 'vector': v} for i, v in enumerate(vectors[:2000])], distance_metric='cosine_distance')
        return {'recall_queries': [normalize(ns.query(rank_by=['vector', 'ANN', v], top_k=10))['rows'] for v in vectors[2000:]]}
    if kind.startswith('copy_'):
        source = name + '-source'
        try:
            write(c.namespace(source), upsert_rows=ROWS, distance_metric='cosine_distance')
            value = source if kind == 'copy_string' else {'source_namespace': source}
            copied = ns.write(copy_from_namespace=value)
            response = copied.model_dump(mode='json', exclude_unset=True)
            if not isinstance(response.get('billing'), dict):
                raise ValueError('copy response missing object billing')
            return {'copy': normalize(response, require_metrics=False), 'after': normalize(ns.query(rank_by=['id', 'asc'], top_k=10, include_attributes=True))}
        finally:
            c.namespace(source).delete_all()
    if kind == 'list_pagination':
        names = [name + '-' + str(i) for i in range(3)]
        try:
            for n in names:
                write(c.namespace(n), upsert_rows=[{'id': 1}])
            cursor = None
            found = []
            seen_cursors = set()
            while True:
                params = {'prefix': name, 'page_size': 2}
                if cursor: params['cursor'] = cursor
                status, body = request('GET', '/v1/namespaces', params=params)
                if status != 200: raise ValueError(f'list: {status} {body}')
                if len(body['namespaces']) > 2:
                    raise ValueError('page_size was not honored')
                found.extend(n['id'] for n in body['namespaces'])
                cursor = body.get('next_cursor')
                if not cursor: break
                if cursor in seen_cursors: raise ValueError('cursor repeated')
                seen_cursors.add(cursor)
            if not seen_cursors: raise ValueError('expected a continuation cursor')
            return {'suffixes': sorted(n.removeprefix(name) for n in found)}
        finally:
            for n in names: c.namespace(n).delete_all()
    setup()
    if kind == 'export_full':
        result, last = [], None
        while True:
            params = {'rank_by': ['id', 'asc'], 'top_k': 2, 'include_attributes': True}
            if last is not None: params['filters'] = ['id', 'Gt', last]
            rows = normalize(ns.query(**params)).get('rows', [])
            if not rows: break
            result.extend(rows)
            last = rows[-1]['id']
        return {'rows': result}
    if kind == 'order_ties':
        write(ns, patch_rows=[{'id': 2, 'n': 1}])
        return normalize(ns.query(rank_by=['n', 'asc'], top_k=10, include_attributes=True))
    if kind == 'raw_invalid_json':
        status, body = request('POST', f'/v2/namespaces/{name}/query', content=b'{', headers={'content-type': 'application/json'})
    elif kind == 'raw_content_type':
        status, body = request('POST', f'/v2/namespaces/{name}/query', content=b'{}', headers={'content-type': 'text/plain'})
    elif kind == 'raw_unknown_route':
        status, body = request('GET', '/v2/not-a-real-route')
    elif kind == 'raw_gzip':
        status, body = request('POST', f'/v2/namespaces/{name}/query', content=gzip.compress(json.dumps({'rank_by': ['id', 'asc'], 'top_k': 2}).encode()), headers={'content-type': 'application/json', 'content-encoding': 'gzip'})
    elif kind == 'raw_unknown_params':
        status, body = request('POST', f'/v2/namespaces/{name}/query?stainless_overload=x', json={'rank_by': ['id', 'asc'], 'top_k': 2})
    elif kind == 'hint_cache_warm':
        status, body = request('GET', f'/v1/namespaces/{name}/hint_cache_warm')
    elif kind == 'debug_recall':
        status, body = request('POST', f'/v1/namespaces/{name}/_debug/recall', json={'num': 1, 'top_k': 2})
    elif kind == 'explain_query':
        status, body = request('POST', f'/v2/namespaces/{name}/explain_query', json={'rank_by': ['id', 'asc'], 'top_k': 2})
        return {'status': status, 'object': isinstance(body, dict), 'plan_text_is_string': isinstance(body, dict) and isinstance(body.get('plan_text'), str), 'error_is_string': isinstance(body, dict) and isinstance(body.get('error'), str)}
    else: raise ValueError(kind)
    if status >= 400:
        from compat.cli import error_category
        return {'status': status, 'category': error_category(str(body.get('error', '')) if isinstance(body, dict) else '')}
    return {'status': status, 'body': normalize(body)}
