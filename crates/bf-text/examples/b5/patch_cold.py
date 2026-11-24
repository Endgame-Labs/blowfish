"""Apply only inside a disposable source copy, and restore both engine files afterward."""
from pathlib import Path
root=Path(__file__).resolve().parents[4]
p=root/'crates/bf-text/src/segment.rs';s=p.read_text();s+='\n#[path = "../examples/b5/cold_install.rs"]\nmod b5_cold_install;\n';p.write_text(s)
p=root/'crates/bf-engine/src/text.rs';s=p.read_text()
a=s.index('    let averaged = |documents:',s.index('async fn corpus_for('))
s=s[:a]+'''    if std::env::var_os("BF_B5_PACK").is_some() {
        return b5_cold::corpus(store,file,terms).await;
    }
'''+s[a:]
s+='\n#[path = "../../bf-text/examples/b5/cold_engine.rs"]\npub(crate) mod b5_cold;\n';p.write_text(s)
p=root/'crates/bf-engine/src/segment.rs';s=p.read_text()
a=s.index('        let index = bf_text::SegmentReader::open_with_header',s.index('pub(crate) async fn text_index('))
b=s.index('        let resident =',a)
s=s[:a]+s[a:b].replace('let index =','let mut index =',1)+'''        if std::env::var_os("BF_B5_PACK").is_some() { index.ensure_lengths().await.map_err(index_error)?; crate::text::b5_cold::install(&mut index,&file.key).await?; }
'''+s[b:];p.write_text(s)
