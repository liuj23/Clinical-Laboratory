import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
import util as utl


def pre_process_volumes_data():
    ref_dept = pd.read_csv('input_data\\reference_tables\\ref_dept_domain.csv')
    tv = pd.read_csv('input_data\\test_volumes\\prov_test_volumes.csv')
    tv = pd.merge(tv, ref_dept, on='Department')
    tv['Month_Year'] = pd.to_datetime(tv['Month_Year'], format='%Y-%m')
    included_service_type = ['Testing']
    excluded_domains = ['Procurement - Exclude', 'Only MHDL Category - Exclude']
    include_filters = {'Service_Type': included_service_type}
    exclude_filters = {'Domain': excluded_domains}
    tv = utl.filter_df(tv, include_filters, include=True)
    tv = utl.filter_df(tv, exclude_filters, include=False)
    agg_col = ['Count']
    # grouping_cols = ['Domain', 'Performing Zone', 'LIS', 'Month_Year']
    grouping_cols = ['Domain', 'Performing Zone', 'Month_Year']
    utl.file_agg_writer(tv, grouping_cols, agg_col, root='pre_processed\\volumes\\')
    return


def pre_process_plc_category_counts():
    tv = pd.read_csv('input_data\\test_volumes\\PLC_category_counts_testing.csv')
    ref_dept = pd.read_csv('input_data\\reference_tables\\ref_dept_domain.csv')
    tv = pd.merge(tv, ref_dept, left_on='DEPARTMENT', right_on='Department')
    tv['Month_Year'] = pd.to_datetime(tv['MONTHID'], format='%Y-%m')
    print tv.columns
    print tv['SUBDISCIPLINE_DETAIL'].unique()
    # included_service_type = ['Testing']
    # included_performing_site = ['Peter Lougheed Centre']
    excluded_departments = ['Respiratory']
    excluded_subdiscipline = ['Blood Gas']
    # include_filters = {'Service_Type': included_service_type, 'Performing Site': included_performing_site}
    exclude_filters = {'DEPARTMENT': excluded_departments, 'SUBDISCIPLINE_DETAIL': excluded_subdiscipline}
    # tv = utl.filter_df(tv, include_filters, include=True)
    tv = utl.filter_df(tv, exclude_filters, include=False)
    agg_col = ['TEST_COUNT']
    grouping_cols = ['Domain', 'Month_Year']
    utl.file_agg_writer(tv, grouping_cols, agg_col, root='pre_processed\\volumes\\PLC\\')


def pre_process_plc_volumes_data():
    ref_dept = pd.read_csv('input_data\\reference_tables\\ref_dept_domain.csv')
    tv = pd.read_csv('input_data\\test_volumes\\prov_test_volumes.csv')
    tv = pd.merge(tv, ref_dept, on='Department')
    tv['Month_Year'] = pd.to_datetime(tv['Month_Year'], format='%Y-%m')
    included_service_type = ['Testing']
    included_performing_site = ['Peter Lougheed Centre']
    excluded_domains = ['Procurement - Exclude', 'Only MHDL Category - Exclude']
    include_filters = {'Service_Type': included_service_type, 'Performing Site': included_performing_site}
    exclude_filters = {'Domain': excluded_domains}
    tv = utl.filter_df(tv, include_filters, include=True)
    tv = utl.filter_df(tv, exclude_filters, include=False)
    agg_col = ['Count']
    # grouping_cols = ['Domain', 'Performing Zone', 'LIS', 'Month_Year']
    grouping_cols = ['Domain', 'Performing Zone', 'Month_Year']
    utl.file_agg_writer(tv, grouping_cols, agg_col, root='pre_processed\\volumes\\')
    return


def pre_process_apqa_data():
    tv = pd.read_csv('input_data\\test_volumes\\APQA_volumes.tsv', sep='\t')
    tv['Month_Year'] = pd.to_datetime(tv['Month_Year'], format='%Y-%m-%d')
    tv['Domain'] = 'AP'
    print tv
    agg_col = ['Samples']
    grouping_cols = ['Domain', 'Zone', 'Month_Year']
    utl.file_agg_writer(tv, grouping_cols, agg_col, root='pre_processed\\volumes\\APQA_samples')
    return


def pre_process_plc_apqa_data():
    tv = pd.read_csv('input_data\\test_volumes\\APQA_volumes.tsv', sep='\t')
    tv['Month_Year'] = pd.to_datetime(tv['Month_Year'], format='%Y-%m-%d')
    tv['Domain'] = 'AP'
    included_performing_site = ['PLC']
    include_filters = {'Site': included_performing_site}
    tv = utl.filter_df(tv, include_filters, include=True)
    print tv
    agg_col = ['Samples']
    grouping_cols = ['Domain', 'Zone', 'Month_Year']
    utl.file_agg_writer(tv, grouping_cols, agg_col, root='pre_processed\\volumes\\APQA_samples')
    return


def pre_process_volumes_billing():
    tv = pd.read_csv('input_data\\test_volumes\\billing_volumes.csv')
    ref_dept = pd.read_csv('input_data\\reference_tables\\ref_dept_domain.csv')
    tv = pd.merge(tv, ref_dept, on='Department')
    tv['year_month'] = pd.to_datetime(tv['year_month'], format='%Y%m')
    print tv
    agg_col = ['Sum Of Count']
    grouping_cols = ['Domain', 'Performing Zone', 'year_month']
    utl.file_agg_writer(tv, grouping_cols, agg_col, root='processed\\volumes\\edmonton\\')


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


def conform_cls_expenses(df):
    # Parse Functional String
    df['org_cd'] = -1
    df['site_cd'] = -1
    df['fn_cd'] = -1
    df[['org_cd', 'site_cd', 'fn_cd']] = utl.parse_bill_code_df(df['AHS Functional String'])
    df['org_cd'] = df['org_cd'].astype(int)
    df['site_cd'] = df['site_cd'].astype(int)
    df['fn_cd'] = df['fn_cd'].astype(np.int64)

    # Load Reference Tables
    ref_cls_cost_grouping = pd.read_csv('input_data/reference_tables/ref_cls_cost_grouping.csv')
    ref_site_muni = pd.read_csv('input_data/reference_tables/ref_site_codes.csv')
    if ref_site_muni['site_cd'].unique().shape[0]  != ref_site_muni.shape[0]:
        print 'WARNING, site-zone reference table has duplicates'
    ref_fn_cd = pd.read_csv('input_data/reference_tables/ref_fn_cd.csv')
    if ref_fn_cd['fn_cd'].unique().shape[0] != ref_fn_cd.shape[0]:
        print 'WARNING, functional_center-department reference table has duplicates'
    ref_dept_domain = pd.read_csv('input_data/reference_tables/ref_dept_domain.csv')
    if ref_dept_domain['Department'].unique().shape[0] != ref_dept_domain.shape[0]:
        print 'WARNING, department-domain reference table has duplicates'
    ref_muni_zone = pd.read_csv('input_data/reference_tables/ref_muni_zone.csv')
    if ref_muni_zone['Municipality'].unique().shape[0] != ref_muni_zone.shape[0]:
        print 'WARNING, municipal-zone reference table has duplicates'
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


def conform_ahs_expenses(df):
    # Parse Functional String
    df['org_cd'] = -1
    df['site_cd'] = -1
    df['fn_cd'] = -1
    df[['org_cd', 'site_cd', 'fn_cd']] = utl.parse_bill_code_df(df['bill_code'])
    df['org_cd'] = df['org_cd'].astype(int)
    df['site_cd'] = df['site_cd'].astype(int)
    df['fn_cd'] = df['fn_cd'].astype(np.int64)

    # Load Reference Tables
    ref_cls_cost_grouping = pd.read_csv('input_data/reference_tables/ref_cls_cost_grouping.csv')
    ref_site_muni = pd.read_csv('input_data/reference_tables/ref_site_codes.csv')
    if ref_site_muni['site_cd'].unique().shape[0]  != ref_site_muni.shape[0]:
        print 'WARNING, site-zone reference table has duplicates'
    ref_fn_cd = pd.read_csv('input_data/reference_tables/ref_fn_cd.csv')
    if ref_fn_cd['fn_cd'].unique().shape[0] != ref_fn_cd.shape[0]:
        print 'WARNING, functional_center-department reference table has duplicates'
    ref_dept_domain = pd.read_csv('input_data/reference_tables/ref_dept_domain.csv')
    if ref_dept_domain['Department'].unique().shape[0] != ref_dept_domain.shape[0]:
        print 'WARNING, department-domain reference table has duplicates'
    ref_muni_zone = pd.read_csv('input_data/reference_tables/ref_muni_zone.csv')
    if ref_muni_zone['Municipality'].unique().shape[0] != ref_muni_zone.shape[0]:
        print 'WARNING, municipal-zone reference table has duplicates'
    ref_muni_zone['Municipality'] = ref_muni_zone['Municipality'].apply(lambda x: str.title(x))
    ref_muni_zone['Zone'] = ref_muni_zone['Zone'].apply(lambda x: str.title(x))

    # Add Table Columns
    # df = pd.merge(df, ref_cls_cost_grouping, on='source_file', how='left')
    # df['cost'] = df['Actual MTD'] * df['multiplier']
    # print 'WARNING: missing cost groupings for source files', df[df['multiplier'].isnull()]['source_file'].unique()
    df = pd.merge(df, ref_site_muni, on='site_cd', how='left')
    print 'WARNING: missing Municipalities for site codes:', df[df['Municipality'].isnull()]['site_cd'].unique()
    df = pd.merge(df, ref_fn_cd, on='fn_cd', how='left')
    print 'WARNING: missing Departments for functional centers:', df[df['Department'].isnull()]['fn_cd'].unique()
    df = pd.merge(df, ref_dept_domain, on='Department', how='left')
    print 'WARNING: missing Domains for site codes:', df[df['Domain'].isnull()]['Department'].unique()
    df = pd.merge(df, ref_muni_zone, on='Municipality', how='left')
    print 'WARNING: missing Zones for Municipality:', df[df['Zone'].isnull()]['Municipality'].unique()
    return df


def pre_process_cls_expenses():
    required_columns = ['Zone', 'Domain', 'cost_type', 'Fiscal Period', 'cost']
    combined = load_cls_expenses()
    combined = conform_cls_expenses(combined)
    combined = combined[required_columns]
    combined = combined.groupby(['Zone', 'Domain', 'Fiscal Period', 'cost_type'])['cost'].sum().unstack('cost_type')
    combined.to_csv('processed\\costs\\cls_expenses.csv')


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
    cds = input['bill_code'].apply(utl.parse_bill_code)
    cds = np.asarray(cds.tolist())
    input['org_cd'] = -1
    input['site_cd'] = -1
    input['fn_cd'] = -1
    input[['org_cd', 'site_cd', 'fn_cd']] = cds
    input['cost'] = input['cost'].fillna(0)
    return input


if __name__ == "__main__":
    # pre_process_volumes_data()
    # df = load_cls_expenses()
    # add_cls_expenses_conformed_columns(df)
    # format_cls_expenses()
    # utl.utl.parse_bill_code('221.0000.71105009975')
    # df = format_ahs_expenses()
    # print conform_ahs_expenses(df)

    # print utl.get_fiscal_year(date(2008, 12, 24))
    # pre_process_cls_expenses()
    # pre_process_apqa_data()
    pre_process_plc_category_counts()
    pre_process_plc_apqa_data()
    # pre_process_volumes_billing()
    # pre_process_plc_volumes_data()