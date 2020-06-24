
mapping = {}
with open('data/string_mapping.tsv', 'r') as fm:    
    for line in fm:
        line = line.strip().split('\t')
        uniprot = line[1].split('|')[0]
        string = line[2]

        mapping[string] = uniprot 
# print(mapping)
data = [['protein_a', 'protein_b', 'score']]
with open('data/string.txt') as f2:
    skip = True
    for line in f2:
        if skip: 
            skip = False
            continue
        line = line.strip().split(' ')
        c = int(line[2])
        if c < 700:
            continue
        a = mapping.get(line[0], 'unknown')
        b = mapping.get(line[1], 'unknown')
        
        data.append([a,b,c])

with open('data/cleaned.csv', 'w') as f:
    for line in data:
        f.write(','.join(map(str, line)) + '\n')