import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date


def get_title(tupleName):
    if type(tupleName) == str:
        return tupleName.replace(' ', '_')
    name = ''
    print tupleName
    for i in tupleName:
        # print i
        name += '_' + str(i)
    return name.replace(' ', '_')


def parse_bill_code(bill_code):
    org_cd, site_cd, fn_cd = bill_code.split('.')
    return [org_cd, site_cd, fn_cd]


def parse_bill_code_df(df):
    cds = df.apply(parse_bill_code)
    cds = np.asarray(cds.tolist())
    return cds


def file_agg_writer(df, grouped_cols, agg_col, root='processed\\test\\'):
    all_cols = grouped_cols + agg_col
    df = df[all_cols]
    grouped = df.groupby(grouped_cols)
    output = grouped.aggregate(np.sum)
    for idx in output.index.map(lambda x: x[:-1]).unique():
        extract = output.ix[idx]
        points = extract.shape[0]
        total = int(np.sum(extract))
        title = root + 'T' + str(total) + '_' + 'P' + str(points) + get_title(idx)
        plt.plot(extract, marker='.')
        plt.xlabel(grouped_cols[-1])
        plt.ylabel(agg_col[0])
        plt.title(get_title(idx))
        plt.savefig(title)
        plt.clf()
        extract['date'] = extract.index
        extract.index = range(points)
        extract['index'] = range(points)
        cols_out = ['index', 'date'] + agg_col
        extract[cols_out].to_csv(title + '.csv', index=False)
    return


def get_fiscal_year(datetime):
    year = datetime.year
    month = datetime.month
    if month < 4:
        return year
    else:
        return year + 1