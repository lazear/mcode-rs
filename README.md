# mcode-rs

A fast implemention of an [MCODE](https://bmcbioinformatics.biomedcentral.com/articles/10.1186/1471-2105-4-2) ("Molecular Complex Detection") related algorithm in Rust. 
This is a relatively simple algorithm that assigns each protein in either a BioPlex or STRING-db input database to a single protein complex, based on a *k-core* graph centrality heuristic. A full STRING database with ~1M edges should be completed in <60s with no intermediate caching of node weights.
