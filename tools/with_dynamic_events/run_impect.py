from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from event_synchronization.with_dynamic_events.impect import ImpectSyncDynamicEventsManager
from event_synchronization.events_utils.impect import ImpectEvents


def load_data(data_dir: Path) -> tuple[dict, pd.DataFrame, dict, pd.DataFrame]:
    """Load the necessary data for Impect/SKC event synchronization."""
    match_data_path = data_dir / 'match_data.json'
    with match_data_path.open() as f:
        match_data = json.load(f)

    match_id = match_data['id']
    skc_events = pd.read_csv(data_dir / f'{match_id}_dynamic_events.csv')

    impect_match_data_path = data_dir / 'impect_match_data.json'
    with impect_match_data_path.open() as f:
        impect_match_data = json.load(f)

    raw_events_path = data_dir / 'raw_events.json'
    with raw_events_path.open() as f:
        raw_impect_events = json.load(f)

    return match_data, skc_events, impect_match_data, raw_impect_events


if __name__ == '__main__':
    data_dir = Path('path/to/your/skc_dynamic_event/and/impect/match/folder')
    output_dir = Path('path/to/load/event/synchronization/results')

    match_data, skc_events, impect_match_data, raw_impect_events = load_data(data_dir)

    impect_events_manager = ImpectEvents(raw_impect_events, impect_match_data, match_data)
    impect_manager = ImpectSyncDynamicEventsManager(skc_events, raw_impect_events, impect_events_manager)

    skc_events_mapping, impect_events_mapping = impect_manager.run()

    skc_events_mapping.to_csv(output_dir / 'skc_events_mapping.csv', index=False)
    impect_events_mapping.to_csv(output_dir / 'impect_events_mapping.csv', index=False)
