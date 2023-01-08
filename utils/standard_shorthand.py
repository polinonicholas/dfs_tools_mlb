def possible_combos(iterable, combo_len=2):
    from itertools import combinations
    return len(list(combinations(list(iterable), combo_len)))
