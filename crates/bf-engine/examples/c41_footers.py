import subprocess, json, struct, sys
import os
B = os.environ["BF_BUCKET"]; pre = os.environ.get("C41_STORE_PREFIX", "tmp/c41/store") + "/ns/msmarco/segments/"
out = {}
segs = subprocess.run(["aws","s3","ls",f"s3://{B}/{pre}"],capture_output=True,text=True).stdout.split()
segs = [s.rstrip("/") for s in segs if s.endswith("/")]
for s in segs:
    key = f"{pre}{s}/fts.1.bft"
    h = json.loads(subprocess.run(["aws","s3api","head-object","--bucket",B,"--key",key],capture_output=True,text=True).stdout or "{}")
    if not h: continue
    size = h["ContentLength"]
    subprocess.run(["aws","s3api","get-object","--bucket",B,"--key",key,"--range",f"bytes={size-72}-{size-1}","foot.bin"],capture_output=True)
    f = open("foot.bin","rb").read()
    ver, flags, docs, tok = struct.unpack_from("<IIII", f, 8); total, lo, mo, do = struct.unpack_from("<QQQQ", f, 24)
    out[s] = dict(size=size, version=ver, docs=docs, total_tokens=total, lengths_offset=lo, metadata_offset=mo, dictionary_offset=do)
json.dump(out, open("sizes.json","w"), indent=1); print(json.dumps(out, indent=1))
