

data = [['protein_a', 'protein_b', 'score']]
with open('data/BioPlex_293T_Network_10K_Dec_2019.tsv') as f2:
    skip = True
    for line in f2:
        if skip: 
            skip = False
            continue
        line = line.strip().split('\t')
        c = int(float(line[-1]) * 1000)
        if c < 700:
            continue
        a = line[2].split('-')[0]
        b = line[3].split('-')[0]
        
        data.append([a,b,c])

with open('data/cleaned_bioplex.csv', 'w') as f:
    for line in data:
        f.write(','.join(map(str, line)) + '\n')