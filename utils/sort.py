# {"fuck": 2, "go": 1, "dude": 3} returns ["go", "fuck", "dude"]
def sort_k_by_v(dict_with_num_values):
    ordered_kv_tuples = sorted(dict_with_num_values.items(), key=lambda pair: pair[1])
    low_high_keys_list = [
        any_key for any_key, num in sorted(ordered_kv_tuples, key=lambda pair: pair[1])
    ]
    return low_high_keys_list


# works for single occurrences, items not in list_to_sort_by are removed
def sort_list_by_list(list_to_return_sorted=[], list_to_sort_by=[]):
    return [v for v in list_to_sort_by if v in list_to_return_sorted]


def sort_list_col_by_list_order(pandas_df, list_col, list_to_sort_by=[]):
    pandas_df[list_col] = pandas_df[list_col].apply(
        lambda x: sort_list_by_list(x, list_to_sort_by)
    )
    return pandas_df
