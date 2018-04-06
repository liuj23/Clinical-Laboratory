import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
import util as utl


def load_volumes_compiled():
    tv = pd.read_csv('model_outputs/volumes_compiled.csv')
    tv['YearMonth'] = pd.to_datetime(tv['YearMonth'], format='%Y-%m-%d')
    excluded_models = ['HWAdd']
    exclude_filters = {'Model': excluded_models}
    tv = utl.filter_df(tv, exclude_filters, include=False)
    return tv

    # agg_col = ['Count']
    # grouped_cols = ['Zone', 'Domain', 'fiscal_year']
    # all_cols = grouped_cols + agg_col
    # tv = tv[all_cols]
    # grouped = tv.groupby(grouped_cols)
    # output = grouped.aggregate(np.sum)
    # print output


def load_calgary_costs():
    costs = pd.read_csv('processed\\costs\\cls_expenses.csv')


    costs['YearMonth'] = pd.to_datetime(costs['Fiscal Period'], format='%Y-%m-%d')
    return costs


def combine_data():
    tv = load_volumes_compiled()
    costs = load_calgary_costs()
    # TRUNCATE 2018 DATA
    costs = costs[costs['YearMonth'] < date(2017, 04, 01)]
    combined = pd.merge(tv, costs, how='outer', on=['Zone', 'Domain', 'YearMonth'])
    included_zones = ['Calgary']
    included_filters = {'Zone': included_zones}
    # combined = utl.filter_df(combined, included_filters, include=True)
    combined['fiscal_year'] = combined['YearMonth'].apply(lambda x: utl.get_fiscal_year(x))
    agg_col = ['Count', 'Labour', 'Other', 'Supplies', 'Total', 'Amortization']
    grouped_cols = ['Zone', 'Domain', 'fiscal_year']
    all_cols = grouped_cols + agg_col
    combined = combined[all_cols]
    grouped = combined.groupby(grouped_cols)
    output = grouped.aggregate(np.sum)

    for idx in output.index.map(lambda x: x[:-1]).unique():
        output.ix[idx][['Labour', 'Other', 'Supplies', 'Total', 'Amortization']] = output.ix[idx][['Labour', 'Other', 'Supplies', 'Total', 'Amortization']].fillna(method='ffill')
        output.ix[idx]['Supplies'] = output.ix[idx].loc[:, 'Count']/output.ix[idx].loc[2017, 'Count']*output.ix[idx]['Supplies']
        # output.ix[idx]['Total'] = 0
        output.ix[idx]['Total'] = np.sum(output.ix[idx][['Labour', 'Other', 'Supplies', 'Amortization']], axis=1)
        print output.ix[idx]['Supplies']
        plt.subplot(211)
        plt.plot(output.ix[idx].index, output.ix[idx]['Total'], label='Total')
        plt.plot(output.ix[idx].index, output.ix[idx]['Other'], label='Other')
        plt.plot(output.ix[idx].index, output.ix[idx]['Labour'], label='Labour')
        plt.plot(output.ix[idx].index, output.ix[idx]['Supplies'], label='Supplies')
        plt.plot(output.ix[idx].index, output.ix[idx]['Amortization'], label='Amortization')
        plt.legend()
        plt.ylabel('Total Cost')
        plt.subplot(212)
        plt.plot(output.ix[idx].index, output.ix[idx]['Count'], label='Volumes')
        plt.xlabel('Fiscal Year')
        plt.ylabel('Volume')
        plt.suptitle(utl.get_title(idx))
        # plt.show()
    print output
    output.to_csv('post_processed\\calgary_cost_projections.csv')
    # output.to_csv('temp.csv')



    # print output.loc['Calgary', 'General Lab', :][['Labour', 'Other', 'Supplies', 'Total', 'Amortization']].fillna(method='ffill')
    # print output['Count']
    # print output[output.index.get_level_values('fiscal_year') == 2017]




if __name__ == "__main__":
    # load_volumes_compiled()
    # load_calgary_costs()
    combine_data()