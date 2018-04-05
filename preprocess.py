import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date


def preprocess_volumes_data():
    ref_dept = pd.read_csv('input_data\\reference_tables\\ref_dept_domain.csv')
    tv = pd.read_csv('input_data\\test_volumes\\prov_test_volumes.csv')
    tv = pd.merge(tv, ref_dept, on='Department')
    tv['Month_Year'] = pd.to_datetime(tv['Month_Year'], format='%Y-%m')
    included_service_type = ['Testing']
    excluded_domains = ['Procurement - Exclude', 'Only MHDL Category - Exclude']
    include_filters = {'Service_Type': included_service_type}
    exclude_filters = {'Domain': excluded_domains}
    for key in include_filters:
        tv = tv[tv[key].isin(include_filters[key])]

    for key in exclude_filters:
        tv = tv[~tv[key].isin(exclude_filters[key])]
    agg_col = ['Count']
    # grouping_cols = ['Domain', 'Performing Zone', 'LIS', 'Month_Year']
    grouping_cols = ['Domain', 'Performing Zone', 'Month_Year']
    file_agg_writer(tv, grouping_cols, agg_col, root='processed\\test\\')
    return


def parse_bill_code(bill_code):
    org_cd, site_cd, fn_cd = bill_code.split('.')
    return [org_cd, site_cd, fn_cd]


def parse_bill_code_df(df):
    cds = df.apply(parse_bill_code)
    cds = np.asarray(cds.tolist())
    return cds


def load_cls_expenses(root_dir='input_data/CLS_expenses/',
                         filenames=['amortization', 'supplies', 'total_excl_amort', 'salaries', 'benefits']):
    combined = None
    for filename in filenames:
        fullname = root_dir + filename + '.csv'
        data = pd.read_csv(fullname)
        data['Fiscal Period'] = data['Fiscal Period'].fillna(method='ffill')
        data['source_file'] = filename
        try:
            data['Fiscal Period'] = pd.to_datetime(data['Fiscal Period'], format='%b-%y')
        except ValueError as e:
            data['Fiscal Period'] = pd.to_datetime(data['Fiscal Period'], format='%y-%b')
        if combined is None:
            combined = data
        else:
            combined = combined.append(data)
    return combined


def add_cls_expenses_conformed_columns(df):
    # Parse Functional String
    df['org_cd'] = -1
    df['site_cd'] = -1
    df['fn_cd'] = -1
    df[['org_cd', 'site_cd', 'fn_cd']] = parse_bill_code_df(df['AHS Functional String'])
    df['org_cd'] = df['org_cd'].astype(int)
    df['site_cd'] = df['site_cd'].astype(int)
    df['fn_cd'] = df['fn_cd'].astype(np.int64)


    # Load Reference Tables
    ref_cls_cost_grouping = pd.read_csv('input_data/reference_tables/ref_cls_cost_grouping.csv')
    ref_site_muni = pd.read_csv('input_data/reference_tables/ref_site_codes.csv')
    ref_fn_cd = pd.read_csv('input_data/reference_tables/ref_fn_cd.csv')
    ref_dept_domain = pd.read_csv('input_data/reference_tables/ref_dept_domain.csv')
    ref_muni_zone = pd.read_csv('input_data/reference_tables/ref_muni_zone.csv')
    ref_muni_zone['Municipality'] = ref_muni_zone['Municipality'].apply(lambda x: str.title(x))
    ref_muni_zone['Zone'] = ref_muni_zone['Zone'].apply(lambda x: str.title(x))

    # Add Table Columns
    df = pd.merge(df, ref_cls_cost_grouping, on='source_file', how='left')
    df['cost'] = df['Actual MTD'] * df['multiplier']
    print 'WARNING: missing cost groupings for source files', df[df['multiplier'].isnull()]['source_file'].unique()
    df = pd.merge(df, ref_site_muni, on='site_cd', how='left')
    print 'WARNING: missing Municipalities for site codes:', df[df['Municipality'].isnull()]['site_cd'].unique()
    df = pd.merge(df, ref_fn_cd, on='fn_cd', how='left')
    print 'WARNING: missing Departments for functional centers:', df[df['Department'].isnull()]['fn_cd'].unique()
    df = pd.merge(df, ref_dept_domain, on='Department', how='left')
    print 'WARNING: missing Domains for site codes:', df[df['Domain'].isnull()]['Department'].unique()
    df = pd.merge(df, ref_muni_zone, on='Municipality', how='left')
    print 'WARNING: missing Zones for Municipality:', df[df['Zone'].isnull()]['Municipality'].unique()
    return df


def get_title(tupleName):
    if type(tupleName) == str:
        return tupleName.replace(' ', '_')
    name = ''
    print tupleName
    for i in tupleName:
        # print i
        name += '_' + str(i)
    return name.replace(' ', '_')


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


def format_cls_expenses():
    reqd_cols = ['cost', 'cost_type', 'Domain', 'Zone', 'Fiscal Period']
    combined = load_cls_expenses()
    combined = add_cls_expenses_conformed_columns(combined)
    agg_col = ['cost']
    grouping_cols = ['cost_type', 'Domain', 'Zone', 'Fiscal Period']
    combined['fiscal_year'] = combined['Fiscal Period'].apply(lambda x: get_fiscal_year(x))
    grouping_cols = ['cost_type', 'Domain', 'Zone', 'fiscal_year']
    file_agg_writer(combined, grouping_cols, agg_col)


def format_ahs_expenses():
    input = pd.read_csv('input_data/AHS_expenses/AHS_expenses.csv')
    for cost_type in input['CostGroup'].unique():
        if '.' not in cost_type:
            print cost_type
            input.loc[input['CostGroup'] == cost_type, 'cost_type'] = cost_type
    input.loc[input['cost_type'].isnull(), 'bill_code'] = input.loc[input['cost_type'].isnull(), 'CostGroup']
    input['bill_code'] = input['bill_code'].fillna(method='ffill')
    # input.loc[input['cost_type'] is None, 'cost_type'] = None
    # input.loc[input['CostGroup']is None, 'cost_type']
    # print input[['CostGroup', 'cost_type', 'bill_code']]
    input = input[input['cost_type'].notnull()]
    # print input.columns
    input = pd.melt(input, id_vars=['cost_type', u'bill_code'], value_vars=[u'16-Jan', u'16-Feb', u'16-Mar', u'15-Apr', u'15-May',
       u'15-Jun', u'15-Jul', u'15-Aug', u'15-Sep', u'15-Oct', u'15-Nov',
       u'15-Dec', u'Unnamed: 13', u'17-Jan', u'17-Feb', u'17-Mar', u'16-Apr',
       u'16-May', u'16-Jun', u'16-Jul', u'16-Aug', u'16-Sep', u'16-Oct',
       u'16-Nov', u'16-Dec', u'Unnamed: 26', u'18-Jan', u'18-Feb', u'18-Mar',
       u'17-Apr', u'17-May', u'17-Jun', u'17-Jul', u'17-Aug', u'17-Sep',
       u'17-Oct', u'17-Nov', u'17-Dec'], var_name='date', value_name='cost')
    cds = input['bill_code'].apply(parse_bill_code)
    cds = np.asarray(cds.tolist())
    input['org_cd'] = -1
    input['site_cd'] = -1
    input['fn_cd'] = -1
    input[['org_cd', 'site_cd', 'fn_cd']] = cds
    input['cost'] = input['cost'].fillna(0)
    return input

def get_fiscal_year(datetime):
    year = datetime.year
    month = datetime.month
    if month < 4:
        return year
    else:
        return year + 1




if __name__ == "__main__":
    preprocess_volumes_data()
    # df = load_cls_expenses()
    # add_cls_expenses_conformed_columns(df)
    # format_cls_expenses()
    # parse_bill_code('221.0000.71105009975')
    # print format_ahs_expenses()

    # print get_fiscal_year(date(2008, 12, 24))