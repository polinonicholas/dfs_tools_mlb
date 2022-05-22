def plural(number, string, possession=False):
    if number < 2:
        return string
    elif number > 1 and possession:
        if string[-1].lower() == "s":
            return string + "'"

        return string + "'" + "s"
    else:
        return string + "s"


# list of player ids to string
def ids_string(id_list):
    return ",".join(str(x) for x in id_list)
