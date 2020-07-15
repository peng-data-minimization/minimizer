import importlib

import pandas as pd
import yaml


def generate_kanon_config(sample: pd.DataFrame, k: int, cn_config: dict, topics: tuple):
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
        if private[prop_name][0] == "*":
            add_subtask("drop_keys", keys=[prop_name])
        elif hierarchy is None:
            pass  # no anonymization applied - do nothing
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
        "task_defaults": {
            "input_offset_reset": "earliest",
            "topic_encoding": "utf8",
            "storage_mode": "memory"
        },
        "tasks": [{
            "name": task_name,
            "function": {
                "signature": task_name.split("-")[0],
                "args": task_config
            }
        } for task_name, task_config in tasks.items()]
    }
    worker_config["tasks"][0]["input_topic"] = topics[0]
    for task, previous_task in zip(worker_config["tasks"][1:], worker_config["tasks"]):
        task["input_topic"] = previous_task["output_topic"] = f"{uuid.uuid4()}"
    worker_config["tasks"][-1]["output_topic"] = topics[1]

    print(textwrap.dedent("""
        To configure k-anonymity in your data processing pipeline, include the following 
        configuration snippet in your config.yml:
        ---
        """))
    print(yaml.dump(worker_config))

    return worker_config


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Config generation util")

    parser.add_argument("--sample-data", required=True, help="the path to the sample data csv file.")
    parser.add_argument("-k", required=True, help="k for k-anonymity.")
    parser.add_argument("--cn-config", required=True, help="name of the py file with initial configuration for "
                                                           "CN-protect library.")
    parser.add_argument("--topics", nargs=2, required=True, help="the names of the in- and the output topic.")

    args = parser.parse_args()

    cn_config = importlib.import_module("kanon_cn_config").cn_config

    generate_kanon_config(pd.read_csv(args.sample_data), args.k, cn_config, tuple(args.topics))
