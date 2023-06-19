import csv
from zipfile import ZipFile, ZIP_DEFLATED


def dict2csv(dictlist, csvfile):
    """
    Takes a list of dictionaries as input and outputs a CSV file.
    """
    keys = dictlist[0].keys()
    unique_rows = set()
    result = []

    for row in dictlist:
        if str(row) in unique_rows:
            continue
        unique_rows.add(str(row))
        result.append(row)

    with open(csvfile, 'w', newline='', encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result)

    with ZipFile(f'{csvfile.split(".")[0]}.zip',
                 'w', ZIP_DEFLATED, compresslevel=5) as zip_object:
        zip_object.write(csvfile)
