import importlib

import pandas as pd
import yaml


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