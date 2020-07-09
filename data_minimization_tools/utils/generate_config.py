import importlib

import pandas as pd
import yaml


def generate_kanon_config(sample: pd.DataFrame, k: int, cn_config: dict):
    """
    Generate a config that contains a set of rules that guarantee k-anonymity on a given dataset. Use these rules
    to apply them to a stream of data.

    :param sample: a pandas dataframe with the sample data
    :param k: the level of k
    :param cn_config: the config for cn_protect
                    (check the `docs <https://docs.cryptonumerics.com/cn-protect-ds/?page=docs.cryptonumerics.com/cn-protect-ds-html/protect.html>`_),
                    the library that applies k-ananymity. Sorry for this not being
                    open source. We are happy if you find a better python implementation of k-anonymity.
    :return: a config that can be fed to the `spi <https://github.com/peng-data-minimization/kafka-spi>`_
    """
    from cn.protect import Protect
    from cn.protect.privacy import KAnonymity
    from cn.protect.hierarchy import DataHierarchy, OrderHierarchy
    import uuid
    import textwrap

    protector = Protect(sample, KAnonymity(k))

    for prop_name, config in cn_config.items():
        protector.itypes[prop_name], protector.hierarchies[prop_name] = config

    private = protector.protect()

    tasks = {}

    def add_subtask(signature: str, **kwargs):
        # todo Don't add a new item to the dict on each call, but instead group tasks that
        #  make use of the same function, e.g.:
        # - signature: drop_keys
        #   args:
        #       keys: [A, B]
        tasks[f"{signature}-{uuid.uuid4()}"] = kwargs

    for prop_name, (identifying, hierarchy) in cn_config.items():
        if hierarchy is None:
            if private[prop_name][0] == "*":
                add_subtask("drop_keys", keys=[prop_name])
            # no anonymization applied - do nothing
        elif isinstance(hierarchy, OrderHierarchy):
            lower, upper = [float(bound) for bound in private[prop_name][0][1:-1].split(",")]
            add_subtask("reduce_to_nearest_value", keys=[prop_name], step_width=upper - lower)
        elif isinstance(hierarchy, DataHierarchy):
            actual_replacements = {}
            possible_replacements = hierarchy.df
            for value in private[prop_name]:
                # todo extract replaced from dataframe
                pass
            add_subtask("replace_with", replacements=actual_replacements)
        else:
            print("Warning: Unsupported hierarchy type " + str(type(hierarchy)))

    worker_config = {
        "tasks": [{
            "name": task_name,
            "input_topic": ":REPLACEME:",
            "input_offset_reset": "earliest",
            "topic_encoding": "utf8",
            "storage_mode": "memory",
            "output_topic": ":REPLACEME:",
            "function": {
                "signature": task_name.split("-")[0],
                "args": task_config
            }
        } for task_name, task_config in tasks.items()]
    }

    print(textwrap.dedent("""
        To configure k-anonymity in your data processing pipeline, include the following 
        configuration snippet in your config.yml:
        ---
        """))
    print(yaml.dump(worker_config))

    return worker_config


def generate_cvdi_config(journey: [dict], config: dict, user_overrides: dict):
    config = {
        # Things we need to change
        "quad_sw_lat": 51.6280977,
        "quad_sw_lng": 10.4713459,
        "quad_ne_lat": 51.9007121,
        "quad_ne_lng": 10.8180638,
        "max_direct_distance": 50.0,
        "max_manhattan_distance": 50.0,
        # Things we are not going to change
        "plot_kml": 0,
        "mf_fit_ext": .5,
        "mf_toggle_scale": 1,
        "mf_scale": 1,
        "n_heading_groups": 36,
        "min_edge_trip_pts": 10,
        "ta_max_q_size": 20,
        "ta_area_width": 30.0,
        "ta_heading_delta": 90,
        "ta_max_speed": 100.0,
        "stop_min_distance": 50.0,
        "stop_max_time": 1.0,
        "stop_max_speed": 2.5,
        "min_direct_distance": 10.0,
        "min_manhattan_distance": 10.0,
        "min_out_degree": 0,
        "max_out_degree": 0,
        "rand_direct_distance": 0,
        "rand_manhattan_distance": 0,
        "rand_out_degree": 0,
        **user_overrides
    }
    return "\n".join([f"{key}:{value}" for key, value in config.items()])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Config generation util")

    parser.add_argument("--sample-data", required=True, help="the path to the sample data csv file.")
    parser.add_argument("-k", required=True, help="k for k-anonymity.")
    parser.add_argument("--cn-config", required=True, help="name of the py file with initial configuration for "
                                                           "CN-protect library.")

    args = parser.parse_args()

    cn_config = importlib.import_module("kanon_cn_config").cn_config

    generate_kanon_config(pd.read_csv(args.sample_data), args.k, cn_config)
