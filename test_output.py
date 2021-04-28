from pathlib import Path

OUTPUT_FILE = Path('./output.txt')

with open(OUTPUT_FILE, 'r') as file:
    lines = file.readlines()

# test category values
categories = []
token_value_strings = []
for i in range(len(lines) - 1):
    kv = lines[i].split()
    values = ' '.join(kv[1:])
    categories.append(kv[0])
    token_value_strings.append(values)

# categories sorted
for i in range(len(categories)):
    sorted_cat = sorted(categories)
    assert categories[i] == sorted_cat[i]

# values sorted in each category
for token_values in token_value_strings:
    token_value = token_values.split()
    assert len(token_value) == 150  # each category should have 150 tokens
    last_chi_square = 1000000000
    for tv in token_value:
        chi_square = float(tv.split(':')[1])
        assert chi_square <= last_chi_square
        last_chi_square = chi_square

# test token values
tokens = lines[-1].split()

assert len(tokens) < 24 * 150
print(f'number unique tokens: {len(tokens)}')

# tokens sorted
for i in range(len(tokens)):
    sorted_t = sorted(tokens)
    assert tokens[i] == sorted_t[i]
