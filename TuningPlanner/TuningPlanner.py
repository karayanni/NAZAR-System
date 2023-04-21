from ExplanationsExtractor import get_explanations_ordered_list, get_attributes_values, reset_counter_factual_drift_col, \
    set_counter_drift_to_zero, GetTotalOutLinersCount
from collections import OrderedDict
from itertools import chain, combinations
import time


attributes_mappings = ['weather', 'location', 'model_type']
vals_attr_mappings = get_attributes_values(attributes_mappings)
diff_attributes = ['weather', 'location', 'id', 'model_type']


def get_explanations_aux():
    return get_explanations_ordered_list(
        attributes=diff_attributes,
        min_occurrences=0.01,
        min_support=0.01,
        min_confidence=0.51,
        min_risk=1.1,
        general_db_filter_query="date <= '2/1/2020' ",
        outliners_sql_filter_query="counter_drift = 1",
        max_length=3,
        debug_print=False)


def did_att_survive(explanation):
    min_support = 0.01
    general_db_filter_query = "date <= '2/1/2020' "
    outliners_sql_filter_query = "counter_drift = 1"

    query_addition = " AND "
    for attribute_value in explanation:
        attribute_key = get_DB_attribute_name_from_value(attribute_value)
        query_addition += f'{attribute_key} = \'{attribute_value}\''

    new_query = general_db_filter_query+query_addition
    total_outliners_count = GetTotalOutLinersCount(outliners_sql_filter_query, outliners_sql_filter_query)
    att_outliners_count = GetTotalOutLinersCount(outliners_sql_filter_query, new_query)
    return (att_outliners_count/total_outliners_count) >= min_support


def get_DB_attribute_name_from_value(attr_value):
    # we map all but id so if not mapped it is id.
    if attr_value not in vals_attr_mappings:
        return "id"
    else:
        return vals_attr_mappings[attr_value]


def index_of(x, e_list_param):
    try:
        return len(x), e_list_param.index(x)
    except:
        return float('inf'), len(e_list_param)


def powerset(iterable):
    """
    powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
    :param iterable: the set to get the power sets of
    :return: a list of all the possible subsets
    """
    s = list(iterable)
    return list(chain.from_iterable(combinations(s, r) for r in range(1, len(s) + 1)))


def get_ordered_dic(e_list_param):
    finetune_dir = OrderedDict()
    for i in e_list_param:
        i_powersets = [j for j in powerset(i) if j in e_list_param]
        if len(i_powersets) != 1:
            i_powersets.remove(i)
            i_powersets = sorted(i_powersets, key=lambda x: index_of(x, e_list_param))

        if i_powersets[0] in finetune_dir:
            if i not in finetune_dir[i_powersets[0]] and i != i_powersets[0]:
                finetune_dir[i_powersets[0]].append(i)
        else:
            if i != i_powersets[0]:
                finetune_dir[i_powersets[0]] = [i]
            else:
                finetune_dir[i_powersets[0]] = []

    for k in finetune_dir.keys():
        if not finetune_dir[k]:
            finetune_dir[k] = [k]

    return finetune_dir


# TODO: reduce complexity by not rerunning DIFF agagin.
def run_counter_factual_analysis(finetune_dir: dict):
    reset_counter_factual_drift_col()

    final_plan_keys = []
    survived_explanations_after_counter_factual_run = set(finetune_dir.keys())

    all_subgroups = set()
    for k, v in finetune_dir.items():
        all_subgroups.update(v)

    survived_sub_groups = all_subgroups
    # iterate over te explanations ordered by risk.
    for explanation in finetune_dir.keys():
        if did_att_survive(explanation):  # in survived_explanations_after_counter_factual_run:
            final_plan_keys.append(explanation)

            for attribute_value in explanation:
                attribute_key = get_DB_attribute_name_from_value(attribute_value)
                set_counter_drift_to_zero(attribute_key, attribute_value)

            # Here now we have the update DB with 0 for counter_drift
            # TODO: switch this with a simple check for the next element instead of the whole DIFF again.
            # explanation_after_cfa = set(get_explanations_aux())

            # survived_explanations_after_counter_factual_run is **tmp_keys**
            # survived_explanations_after_counter_factual_run = explanation_after_cfa.intersection(finetune_dir.keys())
            # survived_sub_groups = explanation_after_cfa.intersection(all_subgroups)

        # in case the explanation doesn't survive the CFA.
        # we still want to check regarding the sub-explanations.
        else:
            # todo: add updated condition with better performance here
            survived_sub_explanations = set(finetune_dir[explanation]).intersection(survived_sub_groups)

            # not empty if a sub-explanation survived the CFA! - if not, do nothing.
            if survived_sub_explanations:

                # we want to add only the most coarse-grained form all the subgroups:
                coarse_survived_subgroups_survived_keys_after_counter_factual_run = survived_explanations_after_counter_factual_run.copy()

                for t_k in survived_explanations_after_counter_factual_run:

                    # if one of the explanation subgroups is more coarse-grained.
                    # for example, if Rain,NY,resnet50 and Rain,NY both are explanations, only add the most coarse.
                    if len(set(powerset(t_k)).intersection(survived_explanations_after_counter_factual_run)) > 1:
                        coarse_survived_subgroups_survived_keys_after_counter_factual_run.remove(t_k)

                final_plan_keys += coarse_survived_subgroups_survived_keys_after_counter_factual_run

                for t_k in coarse_survived_subgroups_survived_keys_after_counter_factual_run:
                    for attribute_value in t_k:
                        attribute_key = get_DB_attribute_name_from_value(attribute_value)
                        set_counter_drift_to_zero(attribute_key, attribute_value)

                # Here now we have the update DB with 0 for counter_drift
                explanation_after_cfa = set(get_explanations_aux())

                survived_explanations_after_counter_factual_run = explanation_after_cfa.intersection(finetune_dir.keys())
                survived_sub_groups = explanation_after_cfa.intersection(all_subgroups)

    final_finetune_plan = OrderedDict({('original',): {}})

    final_plan_keys.reverse()
    for final_exp in final_plan_keys:
        final_finetune_plan[final_exp] = {'subgroups': finetune_dir[final_exp]} if final_exp in finetune_dir else {'subgroups': [final_exp]}

    return final_finetune_plan


def CreateTuningConfigurations(attributes: list[str]):
    global diff_attributes
    diff_attributes = attributes

    print("starting planner - resetting DB settings")
    reset_counter_factual_drift_col()

    e_list_ = get_explanations_aux()

    finetune_dictionary = get_ordered_dic(e_list_)

    finetune_plan = run_counter_factual_analysis(finetune_dictionary)

    return finetune_plan


if __name__ == '__main__':
    reset_counter_factual_drift_col()
    explanation_attributes = ['weather', 'location', 'id', 'model_type']
    start_time = time.time()

    tuning_configurations = CreateTuningConfigurations(explanation_attributes)

    print(f'Tuning Planner Completed in: {time.time() - start_time}  seconds')

    print(tuning_configurations)
