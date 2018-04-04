import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def preprocess_volumes_data():
    ref_dept = pd.read_csv('input_data\dept_domain_ref.csv')
    tv = pd.read_csv('input_data\prov_test_volumes.csv')
    tv = pd.merge(tv, ref_dept, on='Department')
    tv['Month_Year'] = pd.to_datetime(tv['Month_Year'], format='%Y-%m')
    # print tv.columns
    # print tv['Department'].unique()

    included_service_type = ['Testing']
    excluded_domains = ['Procurement - Exclude', 'Only MHDL Category - Exclude']
    include_filters = {'Service_Type': included_service_type}
    exclude_filters = {'Domain': excluded_domains}

    for key in include_filters:
        tv = tv[tv[key].isin(include_filters[key])]

    for key in exclude_filters:
        tv = tv[~tv[key].isin(exclude_filters[key])]

    agg_col = ['Count']
    grouping_cols = ['Domain', 'Performing Zone', 'LIS', 'Month_Year']
    grouping_cols = ['Performing Zone', 'Domain', 'Month_Year']
    grouped = tv.groupby(grouping_cols)

    # print tv['Domain'].unique()
    output = grouped.aggregate(np.sum)
    print output
    domains = output.index.levels[0]
    zones = output.index.levels[1]
    lises = output.index.levels[2]
    i = 1
    for domain in domains:
        for zone in zones:
            try:
                extract = output.loc[domain, zone]
                points = extract.shape[0]
                total = int(np.sum(extract))
                if points > 24:
                    title = domain + ' ' + zone
                    fileout = 'processed\V' + str(total) + '_N' + str(points) + '_' + domain + '_' + zone
                    fileout = fileout.replace(' ', '_')
                    plt.plot(extract, marker='.')
                    plt.xlabel('Year')
                    plt.ylabel('Volume')
                    plt.title(title)
                    plt.savefig(fileout)
                    plt.clf()
                    print 'writing ' + fileout
                    extract['date'] = extract.index
                    extract.index = range(points)
                    extract['index'] = range(points)
                    extract[['index', 'date', 'Count']].to_csv(fileout + '.csv', index=False)
                    i += 1
                else:
                    print 'NOT WRITING ', domain, zone, lis, 'because not enough samples:', points, 'Volume:', total
            except Exception as e:
                    print 'exception', e
                    continue


def format_cls_expenses():
    root_dir = 'input_data/CLS_expenses/'
    reqd_cols = ['cost_type', 'bill_code', 'date', 'cost', 'org_cd', 'site_cd', 'fn_cd']
    filenames = ['amortization', 'supplies', 'total_excl_amort', 'salaries', 'benefits']
    combined = None
    for filename in filenames:
        fullname = root_dir + filename + '.csv'
        data = pd.read_csv(fullname)
        data['Fiscal Period'] = data['Fiscal Period'].fillna(method='ffill')
        data['source_file'] = filename
        if combined is None:
            combined = data
        else:
            combined.append(data)
    cls_cost_grouping = pd.read_csv('input_data/reference_tables/cls_cost_grouping.csv')
    output = pd.merge(combined, cls_cost_grouping, on='source_file', how='left')
    output['cost'] = output['Actual MTD']*output['multiplier']
    cds = output['AHS Functional String'].apply(parse_bill_code)
    cds = np.asarray(cds.tolist())
    output['org_cd'] = -1
    output['site_cd'] = -1
    output['fn_cd'] = -1
    output[['org_cd', 'site_cd', 'fn_cd']] = cds
    output['bill_code'] = output['AHS Functional String']
    output['date'] = output['Fiscal Period']
    ref_dept = pd.read_csv('input_data/reference_tables/site_codes.csv')
    ref_fn_cd = pd.read_csv('input_data/reference_tables/ref_fn_cd.csv')
    output['fn_cd'] = output['fn_cd'].astype(np.int64)
    output = pd.merge(output, ref_fn_cd, on='fn_cd', how='left')
    dept_domain = pd.read_csv('input_data/reference_tables/dept_domain_ref.csv')
    output = pd.merge(output, dept_domain, on='Department', how='left')
    # print output

    output[['fn_cd', 'fn_desc', 'fin_group', 'Department', 'Domain']].to_csv('temp.csv')
    print output.columns


    # print combined[reqd_cols]





    # ammort = pd.read_csv('input_data/CLS_expenses/amortization.csv')
    # # benefits = pd.read_csv('input_data/CLS_expenses/benefits.csv')
    # # salaries = pd.read_csv('input_data/CLS_expenses/salaries.csv')
    # supplies = pd.read_csv('input_data/CLS_expenses/supplies.csv')
    # total = pd.read_csv('input_data/CLS_expenses/total_excl_amort.csv')




    # ammort['cost_type'] = 'ammort'
    # benefits['cost_type'] = 'benefits'
    # salaries['cost_type'] = 'salaries'
    # supplies['cost_type'] = 'supplies'
    # total['cost_type'] = 'total'




    # print ammort.columns
    # print benefits.columns
    # print salaries.columns
    # print supplies.columns
    # print total.columns
    #
    # columns = ['Fiscal Period', 'Actual MTD', 'AHS Functional String']
    # print ammort[columns]
    # print benefits[columns]
    # print salaries[columns]
    # print supplies[columns]
    # print total[columns]

    # print ammort.columns, benefits.columns, salaries.columns, supplies.columns, total.columns
    #
    # print ammort.head()
    # print benefits.head()
    # print salaries.head()
    # print supplies.head()
    # print total.head()


    # cls_costs = pd.read_csv('input_data/CLS_total_expenses.csv', thousands=',')
    # # print cls_costs['AHS_FUNCTIONAL_STR'].unique()
    # # print np.diff(cls_costs.transpose().iloc[3:, 1].astype(float))
    # # cls_costs.iloc[:, :3] = \
    # cls_costs.index = cls_costs['AHS_FUNCTIONAL_STR']
    # output = cls_costs.iloc[:, 3:].stack()
    # output['cost_type'] = cls_costs['ExpenseDetails'][0]
    # print output
    # # output = cls_costs['AHS_FUNCTIONAL_STR'].apply(parse_bill_code)
    # # output = np.asarray(output.tolist())
    # # cls_costs['org_cd'] = -1
    # # cls_costs['site_cd'] = -1
    # # cls_costs['fn_cd'] = -1
    # # cls_costs[['org_cd', 'site_cd', 'fn_cd']] = output
    # #
    # # print cls_costs

def parse_bill_code(bill_code):
    org_cd, site_cd, fn_cd = bill_code.split('.')
    return [org_cd, site_cd, fn_cd]


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











if __name__ == "__main__":
    # preprocess_volumes_data()
    format_cls_expenses()
    # parse_bill_code('221.0000.71105009975')
    # print format_ahs_expenses()

