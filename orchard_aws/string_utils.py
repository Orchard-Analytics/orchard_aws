def list_to_string(string_list=[], delimiter=' '):
    """
        Unpacks list into a single string. When list is emptry, returns ''

        Parameters
        ----------
        string_list: [List of Strings]

        delimeter: String
        Character to join the items in the list on.
        Defualt: ' '

    """
    delimiter_string = "{}".format(delimiter)
    return '' if len(string_list) == 0 else delimiter_string.join(string_list)
