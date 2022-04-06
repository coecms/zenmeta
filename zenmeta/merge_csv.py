import csv
import sys


def read_csv(fname, header=True):
    """Read csv file and return list of rows"""
    csvreader = csv.reader(fname)
    if header is True:
        header = next(csvreader)
    rows = []
    for row in csvreader:
        rows.append(row)
    return rows

def main():
    # assign arguments
    fname1, fname2 = sys.argv[1:3]
    # open input/output files
    f1 = open(fname1, 'r')
    f2 = open(fname2, 'r')
    fout = open('newrecords.csv', 'w')

    # create the csv writer
    writer = csv.writer(fout)
    header = ["id", "title", "url", "type"]
    writer.writerow(header)

    # read the input files
    rows1 = read_csv(f1)
    f1.close()
    rows2 = read_csv(f2)
    f2.close()

    # write first file rows into output
    #for row in rows1:
    #    writer.writerow(row)

    # If row-id in rows2 already exist in rows1 do not add it to output
    ids = [row[0] for row in rows1]

    for row in rows2:
        if row[0] not in ids:
            writer.writerow(row)
    fout.close()

if __name__ == "__main__":
    main()
