"""
Implementation of the Apriori algorithm over a SQL DB. 
The association rule is provided by outliners_sql_filter_query
General SQL filter is also supported - then the item set mining is only ran on the filtered rows.
"""
import mysql.connector
from FrequentItemSetDataClass import FrequentItemSet

# needed if we want to parse the data class to JSON-able object, if so we do:
# jsonsable_result = JSONSerializer.serialize(final_items)
# from dataclasses_serialization.json import JSONSerializer


# If this is set to False, the script will choose Quebec over not Quebec + Fog (in case they have the same metrics)
DELETE_SMALL_DUPLICATE = False


def get_set_to_delete(items1: FrequentItemSet, items2: FrequentItemSet):
    count1 = 0
    count2 = 0

    for val in items1.attributes.values():
        if val != '-':
            count1 += 1

    for val in items2.attributes.values():
        if val != '-':
            count2 += 1
    if DELETE_SMALL_DUPLICATE:
        if count1 < count2:
            return items1
        else:
            return items2

    # THIS IS WEI'S implementations, which keeps the more coarse explanation...
    else:
        if count1 > count2:
            return items1
        else:
            return items2


# TODO: change this to connect to AWS MySQL DB - If needed change schema name and table name accordingly.
def GetDBContext():
    return mysql.connector.connect(user='root', password='nadernader', host='localhost', database='local_schema')


def GetTotalRowsCount(general_db_filter_query):
    cnx = GetDBContext()
    cursor = cnx.cursor()
    cursor.execute("SELECT Count(*) FROM local_schema.drift_log where " + general_db_filter_query)
    res = cursor.fetchone()

    cursor.close()
    cnx.close()

    return res[0]


def GetTotalOutLinersCount(outliners_sql_filter_query, general_db_filter_query):
    cnx = GetDBContext()
    cursor = cnx.cursor()
    cursor.execute(
        "SELECT Count(*) FROM local_schema.drift_log where " + general_db_filter_query + " AND " + outliners_sql_filter_query)
    res = cursor.fetchone()

    cursor.close()
    cnx.close()

    return res[0]


def GetOutLinersCountGroupedByProvidedAttributes(attributes_to_group_by: list | set, outliners_sql_filter_query: str,
                                                 general_db_filter_query: str) -> list[tuple]:
    """
    Queries the DB grouping yb the provided attributes where drift exists.
    :param attributes_to_group_by:
    :return: list of tuples, each with the count of the group and the values of the attributes, example:
    [(1060, 'New South Wales'), (963, 'California'), (1052, 'New York'), (1054, 'Quebec'), (1065, 'Beijing')]
    """
    cnx = GetDBContext()
    cursor = cnx.cursor()
    attr_sql_str = ', '.join(attributes_to_group_by)
    cursor.execute(
        "SELECT COUNT(*)," + attr_sql_str + " FROM local_schema.drift_log where " + general_db_filter_query + " AND " + outliners_sql_filter_query + " group by " + attr_sql_str)
    res = cursor.fetchall()

    cursor.close()
    cnx.close()

    return res


def GetCountInLinersProvidedAttributes(attribute_names: list[str], attribute_values: list[str],
                                       outliners_sql_filter_query: str, general_db_filter_query: str) -> int:
    """
    Queries the DB grouping yb the provided attributes where drift exists.
    :param attribute_values:
    :param attribute_names:
    :return: in_liners_count, out_liners_count
    """
    cnx = GetDBContext()
    cursor = cnx.cursor()
    attr_sql_conditions = []
    for i in range(len(attribute_values)):
        attr_sql_conditions.append(attribute_names[i] + " = '" + attribute_values[i] + "'")

    attr_sql_str = ' AND '.join(attr_sql_conditions)
    cursor.execute(
        "SELECT COUNT(*) FROM local_schema.drift_log where " + attr_sql_str + " AND " + general_db_filter_query + " AND NOT " + outliners_sql_filter_query)
    res = cursor.fetchone()

    cursor.close()
    cnx.close()

    return res[0]


def get_frequent_sets_from_DB(
        attributes: list[str],
        min_occurrences: float,
        min_support: float,
        min_confidence: float,
        min_risk: float,
        outliners_sql_filter_query: str,
        general_db_filter_query: str,
        max_length: int = 8,
        debug_print: bool = True
) -> list[FrequentItemSet]:
    """
    Compute item sets with at-least min_support from transactions by building the item sets bottom up and
    iterating over the transactions to compute the support repeatedly.
    :param general_db_filter_query: this filters the DB and only runs the algorithm on the filtered rows. example date < '2/1/2020'
    :param outliners_sql_filter_query: this query defines the outliners, example: signal_1or2 = 1
    :param min_risk:
    :param min_confidence:
    :param min_support: the minimum percentage of occurrences for the A_out from all the Out_Liner TXS
    :param min_occurrences: the minimum percentage of occurrences for the A_out from all the TXS
    :param attributes: the attributes to consider for the item sets
    :param max_length: the max length of a frequent set
    :param debug_print: whether to print some debug info while running
    :return: a list of the FrequentItemSet Data Class, where the attributes that aren't part of the item
    set have the value '-'.
    """
    if not (isinstance(min_occurrences, float) and (0 <= min_occurrences <= 1)):
        raise ValueError("`min_occurrences` must be a number between 0 and 1.")

    if not (isinstance(min_support, float) and (0 <= min_support <= 1)):
        raise ValueError("`min_support` must be a number between 0 and 1.")

    if not (isinstance(min_confidence, float) and (0 <= min_confidence <= 1)):
        raise ValueError("`min_confidence` must be a number between 0 and 1.")

    total_number_of_rows = GetTotalRowsCount(general_db_filter_query)
    total_number_of_out_liners = GetTotalOutLinersCount(outliners_sql_filter_query, general_db_filter_query)

    if total_number_of_rows < 1:
        raise Exception("DB is Empty")

    # no out_liners --> no work needed
    if total_number_of_out_liners < 1:
        return []

    min_number_of_outliners_for_occurrence_condition = total_number_of_rows * min_occurrences
    min_number_of_outliners_for_supp_condition = total_number_of_out_liners * min_support

    min_num_of_outliners = max(min_number_of_outliners_for_supp_condition,
                               min_number_of_outliners_for_occurrence_condition)

    # example of attr_values_with_high_support_by_set_size (contains only the sets of values with high enough support):
    # {1: [
    #   (['weather'], ['clear-day', 'rain']),
    #   (['location'], ['New South Wales', 'California', 'New York', 'Quebec', 'Beijing', 'Tibet', 'United Kingdom', 'Worst20classes']),
    #   (['model_type'], ['Resnet50'])
    #   ]}
    attr_values_with_high_support_by_set_size = {}

    k = 1
    attr_values_with_high_support_by_set_size[k] = []
    final_items = []

    for attr in attributes:
        attr_counts = GetOutLinersCountGroupedByProvidedAttributes([attr], outliners_sql_filter_query,
                                                                   general_db_filter_query)
        curr_attr_vals = []
        for attr_val in attr_counts:
            # attr_val looks like: (1060, 'New South Wales')

            curr_out_liners_count = attr_val[0]

            # this is a check for min_occurrences and min_support
            if curr_out_liners_count > min_num_of_outliners:
                curr_attr_vals.append([attr_val[1]])

                curr_in_liners_count = GetCountInLinersProvidedAttributes([attr], [attr_val[1]],
                                                                          outliners_sql_filter_query,
                                                                          general_db_filter_query)

                # this is the min confidence check:
                curr_confidence = (curr_out_liners_count / (curr_in_liners_count + curr_out_liners_count))
                if curr_confidence >= min_confidence:

                    b_o = total_number_of_out_liners - curr_out_liners_count

                    # to prevent dividing by 0 in case b_o == 0, this shouldn't happen
                    if b_o == 0:
                        raise Exception("support not added for inf risk - shouldn't happen")
                    else:
                        total_in_liners_count = total_number_of_rows - total_number_of_out_liners
                        b_i = total_in_liners_count - curr_in_liners_count
                        curr_risk = curr_confidence / (b_o / (b_o + b_i))
                        # this is the min risk check:
                        if curr_risk >= min_risk:
                            # we append the attribute's value
                            final_attrs = {}
                            for attribute in attributes:
                                if attribute == attr:
                                    final_attrs[attribute] = attr_val[1]
                                else:
                                    final_attrs[attribute] = '-'

                            final_items.append(FrequentItemSet(risk_ratio=curr_risk,
                                                               occurrence_ratio=(
                                                                       curr_out_liners_count / total_number_of_rows),
                                                               support_ratio=(
                                                                       curr_out_liners_count / total_number_of_out_liners),
                                                               confidence=curr_confidence,
                                                               attributes=final_attrs))

        if len(curr_attr_vals) > 0:
            attr_values_with_high_support_by_set_size[k].append(([attr], curr_attr_vals))

    k += 1
    while k <= max_length and k <= len(attributes):
        attr_values_with_high_support_by_set_size[k] = []
        #
        # for attr_set1 in attr_values_with_high_support_by_set_size[k - 1]:
        #     for attr_set2 in attr_values_with_high_support_by_set_size[k - 1]:
        visited = set()

        for i in range(len(attr_values_with_high_support_by_set_size[k - 1])):
            j = i + 1
            while j < len(attr_values_with_high_support_by_set_size[k - 1]):
                attr_set1 = attr_values_with_high_support_by_set_size[k - 1][i]
                attr_set2 = attr_values_with_high_support_by_set_size[k - 1][j]
                combined_set_of_attributes = set(attr_set1[0] + attr_set2[0])
                if len(combined_set_of_attributes) != k or combined_set_of_attributes.__hash__ in visited:
                    j += 1
                    continue
                else:
                    visited.add(combined_set_of_attributes.__hash__)
                    # here we know we have two lists of attributes with k-1 common fields.
                    # TODO: consider query by each supported value and not the attributes and compare performance
                    attr_counts = GetOutLinersCountGroupedByProvidedAttributes(combined_set_of_attributes,
                                                                               outliners_sql_filter_query,
                                                                               general_db_filter_query)

                    curr_attr_vals = []
                    for attr_val in attr_counts:
                        # attr_val looks like: (1060, 'New South Wales', 'rain')
                        curr_out_liners_count = attr_val[0]

                        # this is a check for min_supp, min_occurrences
                        if curr_out_liners_count > min_num_of_outliners:
                            # we append the attribute's values
                            list_of_attribute_vals = list(attr_val)[1:]
                            list_of_attribute_names = list(combined_set_of_attributes)

                            # add to high support items (frequent) for FIM.
                            curr_attr_vals.append(list_of_attribute_vals)

                            curr_in_liners_count = GetCountInLinersProvidedAttributes(list_of_attribute_names,
                                                                                      list_of_attribute_vals,
                                                                                      outliners_sql_filter_query,
                                                                                      general_db_filter_query)
                            # this is the min confidence check:
                            curr_confidence = (curr_out_liners_count / (curr_in_liners_count + curr_out_liners_count))

                            if curr_confidence >= min_confidence:
                                b_o = total_number_of_out_liners - curr_out_liners_count

                                # to prevent dividing by 0 in case b_o == 0
                                if b_o == 0:
                                    curr_attr_vals.append(list_of_attribute_vals)
                                else:
                                    total_in_liners_count = total_number_of_rows - total_number_of_out_liners
                                    b_i = total_in_liners_count - curr_in_liners_count
                                    curr_risk = curr_confidence / (b_o / (b_o + b_i))
                                    # this is the min risk check:
                                    if curr_risk >= min_risk:
                                        # we append the attribute's value
                                        final_attrs = {}
                                        for i in range(len(list_of_attribute_names)):
                                            final_attrs[list_of_attribute_names[i]] = list_of_attribute_vals[i]
                                        for attribute in attributes:
                                            if attribute not in final_attrs:
                                                final_attrs[attribute] = '-'

                                        final_items.append(FrequentItemSet(risk_ratio=curr_risk,
                                                                           occurrence_ratio=(
                                                                                   curr_out_liners_count / total_number_of_rows),
                                                                           support_ratio=(
                                                                                   curr_out_liners_count / total_number_of_out_liners),
                                                                           confidence=curr_confidence,
                                                                           attributes=final_attrs))

                    if len(curr_attr_vals) > 0:
                        attr_values_with_high_support_by_set_size[k].append(
                            (list(combined_set_of_attributes), curr_attr_vals))

                j += 1

        k += 1

    if debug_print:
        print(f'the final frequent item sets in the DB with min occurrences {min_occurrences}:')
        print(attr_values_with_high_support_by_set_size)
        print("------------------------------------------------------------------------------------------")
        print("final items:")
        print(final_items)

    final_items.sort()
    return final_items


def get_explanations_ordered_list(attributes: list[str],
                                  min_occurrences: float,
                                  min_support: float,
                                  min_confidence: float,
                                  min_risk: float,
                                  outliners_sql_filter_query: str,
                                  general_db_filter_query: str,
                                  max_length: int = 8,
                                  debug_print: bool = True) -> list[tuple[str]]:
    """
    This returns exactly what Wei's code expects e_list_ to contain.
    """
    frequent_outliners_item_sets = get_frequent_sets_from_DB(attributes,
                                                             min_occurrences,
                                                             min_support,
                                                             min_confidence,
                                                             min_risk,
                                                             outliners_sql_filter_query,
                                                             general_db_filter_query,
                                                             max_length,
                                                             debug_print)

    for i in range(len(frequent_outliners_item_sets)):
        j = i + 1
        while j < len(frequent_outliners_item_sets):
            if frequent_outliners_item_sets[i].confidence == frequent_outliners_item_sets[j].confidence \
                    and frequent_outliners_item_sets[i].risk_ratio == frequent_outliners_item_sets[j].risk_ratio \
                    and frequent_outliners_item_sets[i].occurrence_ratio == frequent_outliners_item_sets[
                j].occurrence_ratio \
                    and frequent_outliners_item_sets[i].support_ratio == frequent_outliners_item_sets[j].support_ratio:
                frequent_outliners_item_sets.remove(
                    get_set_to_delete(frequent_outliners_item_sets[i], frequent_outliners_item_sets[j]))

            j += 1

    explanations_list = []

    for frequent_item_set in frequent_outliners_item_sets:
        vals_list = []
        for val in frequent_item_set.attributes.values():
            if val != '-':
                vals_list.append(val)

        explanations_list.append(tuple(vals_list))

    explanations_list.reverse()
    return explanations_list


def get_distinct_values_of_field(att: str):
    cnx = GetDBContext()
    cursor = cnx.cursor()
    cursor.execute(
        "SELECT DISTINCT " + att + " FROM local_schema.drift_log")
    res = cursor.fetchall()

    cursor.close()
    cnx.close()
    return res


def reset_counter_factual_drift_col():
    cnx = GetDBContext()
    cursor = cnx.cursor()
    cursor.execute("UPDATE local_schema.drift_log SET counter_drift = signal_1or2")
    res = cursor.fetchall()

    cnx.commit()
    cursor.close()
    cnx.close()
    return res


def set_counter_drift_to_zero(attribute_name, attribute_value):
    cnx = GetDBContext()
    cursor = cnx.cursor()
    cursor.execute(
        "UPDATE local_schema.drift_log SET counter_drift = '0' WHERE " + attribute_name + " = '" + attribute_value + "'")
    res = cursor.fetchall()

    cnx.commit()
    cursor.close()
    cnx.close()
    return res


def get_attributes_values(attributes: list[str]):
    """
    :return: a mapping from each possible value in the DB to it's attribute
    for example, rain --> weather
    """
    mappings = {}

    for attr in attributes:
        vals = get_distinct_values_of_field(attr)
        for val in vals:
            mappings[val[0]] = attr

    return mappings


if __name__ == '__main__':
    e_list_ = get_explanations_ordered_list(
        attributes=['weather', 'location', 'id', 'model_type'],
        min_occurrences=0.01,
        min_support=0.01,
        min_confidence=0.51,
        min_risk=1.1,
        general_db_filter_query="date <= '2/1/2020' ",
        outliners_sql_filter_query="signal_1or2 = 1",
        max_length=3,
        debug_print=False)

    print(e_list_)
