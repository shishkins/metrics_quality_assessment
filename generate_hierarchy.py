from get_dataframes import get_data

hierarchy_df = get_data()['hierarchy_df']
def generate_hierarchy(hierarchy_df):
    # {
    #     'title': 'Parent',
    #     'key': '0',
    #     'children': [{
    #         'title': 'Child',
    #         'key': '0-0',
    #         'children': [
    #             {'title': 'Subchild1', 'key': '0-0-1'},
    #             {'title': 'Subchild2', 'key': '0-0-2'},
    #             {'title': 'Subchild3', 'key': '0-0-3'},
    #         ],
    #     }]}
    return hierarchy_df['department_name'].unique()


print(generate_hierarchy(hierarchy_df))