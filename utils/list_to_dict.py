def list_to_dict(list_data):
    report = dict()
    for row in list_data:
        report[row['id']] = {key: value for key, value in row.items()}

    return report