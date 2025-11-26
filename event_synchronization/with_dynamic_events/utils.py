from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd
from dateutil.parser import parse

GIA_BACK_OFFSET = 5  # frames before end
GIA_WINDOW = 50  # window size
GIB_BACK_WINDOW = 50  # frames before end
GIB_FORWARD_OFFSET = 5  # frames after start of back window

# -----------------------------
# Shared constants (tunable)
# -----------------------------
PP_TOL = 25
PRESS_TOL_statsbomb = 50
DUEL_TOL_statsbomb = 20

FIRST_TOUCH_statsbomb = ['Ball Receipt*', 'Ball Recovery', 'Interception']
LAST_TOUCH_statsbomb = ['Pass', 'Shot', 'Clearance', 'Dispossessed', 'Miscontrol', 'Goal Keeper']
POSSESSION_statsbomb = ['Carry', 'Dribble', 'Ball Recovery']
COL_TYPE_statsbomb = 'type_name'
DUEL_NAME_statsbomb = 'Duel'

DUEL_TOL_impect = 40
FIRST_TOUCH_impect = ['RECEPTION', 'LOOSE_BALL_REGAIN', 'INTERCEPTION', 'GROUND_DUEL', 'BLOCK']
LAST_TOUCH_impect = ['PASS', 'SHOT', 'CLEARANCE', 'DRIBBLE']
COL_TYPE_impect = 'actionType'
DUEL_NAME_impect = 'GROUND_DUEL'


FIRST_TOUCH_wyscout = ['touch', 'interception']
LAST_TOUCH_wyscout = ['pass', 'shot', 'clearance', 'duel', 'acceleration']
COL_TYPE_wyscout = 'type_primary'
DUEL_NAME_wyscout = 'duel'


def get_match_name(match_data: dict) -> str:
    home_team_short_name = match_data['home_team']['short_name']
    away_team_short_name = match_data['away_team']['short_name']
    match_date_time = parse(match_data['date_time']).strftime('%Y%m%d')
    return f'{match_date_time}_{home_team_short_name}_{away_team_short_name}'


def get_match_id_from_match_name(match_name: str, skc_client) -> int:  # noqa: ANN001
    min_date = parse(match_name.split('_')[0])
    max_date = min_date + timedelta(days=1)

    matches_data = skc_client.get_matches(
        params={'date_time__gte': min_date.isoformat(), 'date_time__lte': max_date.isoformat()}
    )

    for match_data in matches_data:
        if match_name == get_match_name(match_data):
            return match_data['id']
    msg = f'match_name {match_name} not found'
    raise ValueError(msg)


def get_type_provider_id(provider: str) -> type:
    if provider == 'statsbomb':
        return str
    if provider in ['impect', 'wyscout']:
        return int
    msg = f"Unknown provider '{provider}' for type_provider_id"
    raise ValueError(msg)


def _format_player(pid: int | None, skc_ply_id_to_skc_ply: dict) -> str | None:
    """Format player as 'short-number' if possible, else short name, else None."""
    if pd.isna(pid):
        return None
    info = skc_ply_id_to_skc_ply.get(pid, {})
    short = info.get('short_name')
    num = info.get('number') or info.get('jersey_number')
    if short and num:
        return f'{short}-{num}'
    return short


def format_output_mapping_pp_provider(mapping_df: pd.DataFrame, provider: str) -> pd.DataFrame:
    ft_col = f'{provider}_FT_id'
    poss_col = f'{provider}_possessions_ids'
    lt_col = f'{provider}_LT_id'
    out_col = f'{provider}_id'

    frames = []

    # 1) FT
    if ft_col in mapping_df.columns:
        ft = mapping_df[['event_id', ft_col]].dropna(subset=[ft_col]).rename(columns={ft_col: out_col})
        frames.append(ft[['event_id', out_col]])

    # 2) possessions (several lines)
    if poss_col in mapping_df.columns:
        poss = mapping_df[['event_id', poss_col]].copy()
        poss = poss.explode(poss_col, ignore_index=True).rename(columns={poss_col: out_col})
        poss = poss.dropna(subset=[out_col])[['event_id', out_col]]

        frames.append(poss)

    # 3) LT
    if lt_col in mapping_df.columns:
        lt = mapping_df[['event_id', lt_col]].dropna(subset=[lt_col]).rename(columns={lt_col: out_col})
        frames.append(lt[['event_id', out_col]])

    if not frames:
        return pd.DataFrame(columns=['event_id', out_col])

    links = pd.concat(frames, ignore_index=True)
    links = links.dropna(subset=['event_id', out_col]).drop_duplicates().reset_index(drop=True)
    links[out_col] = links[out_col].astype(get_type_provider_id(provider))
    return links.dropna(subset=[out_col]).copy()


def format_output_mapping_skc_events(
    mapping_df: pd.DataFrame,
    skc_events: pd.DataFrame,
    matched_duels: pd.DataFrame,
    provider: str,
    pressure_confirmed: pd.DataFrame = None,
) -> pd.DataFrame:
    skc_events_mapping0 = skc_events.merge(
        mapping_df[['event_id', f'{provider}_FT_id', f'{provider}_possessions_ids', f'{provider}_LT_id', 'pattern']],
        on='event_id',
        how='left',
    )
    skc_events_mapping1 = skc_events_mapping0.merge(matched_duels, on='event_id', how='left').rename(
        columns={f'{provider}_id': f'{provider}_duel_id'}
    )
    skc_events_mapping1.loc[skc_events_mapping1[f'{provider}_duel_id'].notna(), 'pattern'] = globals()[
        f'DUEL_NAME_{provider}'
    ]
    match_mask = (
        skc_events_mapping1[f'{provider}_duel_id'].notna()
        | skc_events_mapping1[f'{provider}_FT_id'].notna()
        | skc_events_mapping1[f'{provider}_LT_id'].notna()
        | skc_events_mapping1[f'{provider}_possessions_ids'].notna()
    )

    if pressure_confirmed is not None:
        skc_events_mapping = skc_events_mapping1.merge(pressure_confirmed, on='event_id', how='left').rename(
            columns={f'{provider}_id': f'{provider}_pressure_id'}
        )
        mask_both = (
            skc_events_mapping[f'{provider}_duel_id'].notna() & skc_events_mapping[f'{provider}_pressure_id'].notna()
        )
        skc_events_mapping.loc[mask_both, 'pattern'] = 'Pressure-Duel'
        mask_pressure_only = skc_events_mapping[f'{provider}_pressure_id'].notna() & ~mask_both
        skc_events_mapping.loc[mask_pressure_only, 'pattern'] = 'Pressure'
        match_mask = (
            skc_events_mapping[f'{provider}_duel_id'].notna()
            | skc_events_mapping[f'{provider}_pressure_id'].notna()
            | skc_events_mapping[f'{provider}_FT_id'].notna()
            | skc_events_mapping[f'{provider}_LT_id'].notna()
            | skc_events_mapping[f'{provider}_possessions_ids'].notna()
        )
        skc_events_mapping.loc[match_mask, 'dynamic_matched'] = True
        return skc_events_mapping
    skc_events_mapping1.loc[match_mask, 'dynamic_matched'] = True
    return skc_events_mapping1


def enriched_po_and_obr(skc_events_mapping: pd.DataFrame, provider: str) -> pd.DataFrame:
    """Enrich PO and OBR events with player possession matching events."""
    df = skc_events_mapping

    cols = [f'{provider}_FT_id', f'{provider}_possessions_ids', f'{provider}_LT_id']
    pp = (
        df.loc[df.event_type == 'player_possession', ['event_id', *cols]]
        .assign(_k=lambda d: d['event_id'].astype(str))
        .set_index('_k')[cols]
    )
    obr_po = ['passing_option', 'off_ball_run']
    mask = df['event_type'].isin(obr_po) & df['associated_player_possession_event_id'].notna()
    keys = df.loc[mask, 'associated_player_possession_event_id'].astype(str)
    df.loc[mask, cols] = pp.reindex(keys).values
    return df


def apply_sb_output_format(
    provider_events: pd.DataFrame,
    skc_events: pd.DataFrame,
    mapping_df: pd.DataFrame,
    matched_duels: pd.DataFrame,
    pressure_confirmed: pd.DataFrame,
) -> pd.DataFrame:
    """Apply output format to the mapping DataFrame."""
    player_possession_mapping = format_output_mapping_pp_provider(mapping_df, 'statsbomb')
    matched_duels = matched_duels[['event_id', 'id']].rename(columns={'id': 'statsbomb_id'})
    matched_pressure = pressure_confirmed[['event_id', 'id']].rename(columns={'id': 'statsbomb_id'})
    matching_events = pd.concat([player_possession_mapping, matched_duels, matched_pressure])
    provider_events_mapping = provider_events.merge(matching_events, left_on='id', right_on='statsbomb_id', how='left')

    provider_events_mapping.loc[provider_events_mapping['event_id'].notna(), 'dynamic_matched'] = True

    skc_events_mapping = format_output_mapping_skc_events(
        mapping_df, skc_events, matched_duels, 'statsbomb', matched_pressure
    )
    return enriched_po_and_obr(skc_events_mapping, 'statsbomb'), provider_events_mapping


def apply_output_format(
    provider_events: pd.DataFrame,
    skc_events: pd.DataFrame,
    mapping_df: pd.DataFrame,
    matched_duels: pd.DataFrame,
    provider: str,
) -> pd.DataFrame:
    """Apply output format to the mapping DataFrame."""
    player_possession_mapping = format_output_mapping_pp_provider(mapping_df, provider)
    matched_duels = matched_duels[['event_id', 'id']].rename(columns={'id': f'{provider}_id'})
    matching_events = pd.concat([player_possession_mapping, matched_duels])
    provider_events_mapping = provider_events.merge(
        matching_events, left_on='id', right_on=f'{provider}_id', how='left'
    )
    provider_events_mapping.loc[provider_events_mapping['event_id'].notna(), 'dynamic_matched'] = True

    skc_events_mapping = format_output_mapping_skc_events(mapping_df, skc_events, matched_duels, provider)
    return enriched_po_and_obr(skc_events_mapping, provider), provider_events_mapping


def identify_game_interruption_events(
    mapping_bundles: pd.DataFrame, provider_events: pd.DataFrame
) -> tuple[list[int], pd.DataFrame]:
    """Annotate game interruption (gia/gib) on events and return affected IDs."""
    events_game_interrupt: list[int] = []
    skc_test = provider_events.copy()
    for _, row in mapping_bundles.iterrows():
        if not pd.isna(row['gia']):
            frame_start = row['frame_end'] - GIA_BACK_OFFSET
            end_ = frame_start + GIA_WINDOW
            mask = (skc_test['skc_frame'] >= frame_start) & (skc_test['skc_frame'] <= end_)
            events = skc_test[mask]
            skc_test.loc[mask, 'gia'] = row['gia']
            events_game_interrupt.extend(events['id'].tolist())

        if not pd.isna(row['gib']):
            frame_start = row['frame_end'] - GIB_BACK_WINDOW
            end_ = frame_start + GIB_FORWARD_OFFSET
            mask = (skc_test['skc_frame'] >= frame_start) & (skc_test['skc_frame'] <= end_)
            events = skc_test[mask]
            skc_test.loc[mask, 'gib'] = row['gib']
            events_game_interrupt.extend(events['id'].tolist())

    return events_game_interrupt, skc_test


def _extract_related_info_sb(
    row: pd.Series,
    sb_events_sync: pd.DataFrame,
) -> tuple[str | None, int | None, str | None, int | None]:
    related_events = row.get('related_events')
    if not related_events or not isinstance(related_events, list):
        return None, None, None, None

    event = str(related_events[0])
    # Compare as string to handle mixed dtypes for StatsBomb ids
    re = sb_events_sync[sb_events_sync['id'].astype(str) == event]

    if re.empty:
        return None, None, None, None

    re_player_name = re['player_name'].values[0] if 'player_name' in re.columns else None
    re_frame = int(re['skc_frame'].values[0]) if 'skc_frame' in re.columns else None
    re_type = re['type_name'].values[0] if 'type_name' in re.columns else None
    re_ply_id = (
        int(re['player_id'].values[0]) if 'player_id' in re.columns and pd.notna(re['player_id'].values[0]) else None
    )
    return re_player_name, re_frame, re_type, re_ply_id


def preprocess_sb_events(sb_events_sync: pd.DataFrame, skc_events: pd.DataFrame) -> tuple[Any, Any, Any, Any, Any, Any]:  # noqa: C901
    """Prepare SB and SKC frames for matching use-cases (PP, pressure, duels).

    Returns: (sb_events_sync, sb_three, pp_sorted, sb_pressure, obe_sorted, sb_duels)
    """
    sb_events_sync = sb_events_sync.rename(
        columns={
            'event_id': 'sb_event_id',
            'player_id': 'sb_player_id',
            'skc_player_id': 'player_id',
        }
    ).copy()

    # Expand related event info
    re_cols = sb_events_sync.apply(
        lambda r: pd.Series(
            _extract_related_info_sb(r, sb_events_sync),
            index=['re_player_name', 're_skc_frame', 're_type', 're_player_id'],
        ),
        axis=1,
    )
    sb_events_sync = pd.concat([sb_events_sync, re_cols], axis=1)

    # Subsets
    pp = skc_events.query("event_type == 'player_possession'").copy()
    obe = skc_events.query("event_type == 'on_ball_engagement'").copy()

    sb = sb_events_sync.copy()

    # Basic sanity columns
    for col in ['event_id', 'player_id', 'frame_start']:
        if col not in pp.columns:
            msg = f"Missing column '{col}' in player_possession (SKC) dataframe"
            raise ValueError(msg)

    if ('skc_frame' not in sb.columns) and ('timestamp' not in sb.columns):
        msg = "SB doit contenir 'skc_frame' ou 'timestamp'."
        raise ValueError(msg)
    if 'type_name' not in sb.columns:
        msg = "SB doit contenir 'type_name' (renommé en 'type_name')."
        raise ValueError(msg)

    # Filter SB to actionable types for PP mapping
    sb_three = sb[sb['type_name'].isin(FIRST_TOUCH_statsbomb + LAST_TOUCH_statsbomb + POSSESSION_statsbomb)].copy()
    set_pieces = ['Corner', 'Free Kick', 'Goal Kick', 'Throw-in']
    sb_three = sb_three[
        (sb_three['ball_receipt_outcome_name'] != 'Incomplete') & (~sb_three['pass_type_name'].isin(set_pieces))
    ].copy()

    cols_pp = [
        'event_id',
        'player_name',
        'player_id',
        'frame_start',
        'frame_physical_start',
        'frame_end',
        'end_type',
        'player_in_possession_id',
        'player_in_possession_name',
        'event_subtype',
        'game_interruption_before',
        'game_interruption_after',
    ]
    cols_sb = [
        'type_name',
        'player_id',
        'skc_frame',
        'pass_height_name',
        'pass_body_part_name',
        'id',
        'player_name',
        'related_events',
        're_player_name',
        're_skc_frame',
        're_type',
        're_player_id',
        'duel_type_name',
        'under_pressure',
    ]

    pp_sorted = pp.sort_values(['frame_start'])[list(set(cols_pp))].reset_index(drop=True)
    obe_sorted = obe.sort_values(['frame_start'])[list(set(cols_pp))].reset_index(drop=True)

    sb_sorted = sb_three.sort_values(['skc_frame'])[list(set(cols_sb) & set(sb_three.columns))].reset_index(drop=True)
    sb_pressure = sb.query("type_name == 'Pressure'").copy()
    sb_duels = sb.query("type_name == 'Duel'").copy()

    # Dtypes
    def to_int_safe(df: pd.DataFrame, column: str) -> None:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')

    for df, cols in [
        (sb_sorted, ['skc_frame', 'player_id']),
        (pp_sorted, ['frame_start']),
        (obe_sorted, ['frame_start', 'frame_physical_start']),
        (sb_pressure, ['skc_frame', 'player_id']),
        (sb_duels, ['skc_frame', 'player_id']),
    ]:
        for c in cols:
            to_int_safe(df, c)

    sb_pressure = sb_pressure[['id', 'type_name', 'player_id', 'skc_frame', 'related_events']].copy()
    sb_duels = sb_duels[
        [
            'id',
            'type_name',
            'player_id',
            'player_name',
            'skc_frame',
            'related_events',
            'duel_type_name',
            'under_pressure',
            're_player_name',
            're_skc_frame',
            're_type',
            're_player_id',
        ]
    ].copy()

    # Cast to plain ints for merge_asof
    for df, cols in [
        (sb_sorted, ['skc_frame', 'player_id']),
        (pp_sorted, ['frame_start']),
        (obe_sorted, ['frame_start', 'frame_physical_start']),
        (sb_pressure, ['skc_frame', 'player_id']),
        (sb_duels, ['skc_frame', 'player_id']),
    ]:
        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype('int64', errors='ignore')

    return sb_events_sync, sb_three, pp_sorted, sb_pressure, obe_sorted, sb_duels


def preprocess_prov_events(
    events_sync: pd.DataFrame, skc_events: pd.DataFrame, provider: str
) -> tuple[Any, Any, Any, Any]:
    """Prepare WYSCOUT or IMPECT and SKC frames for matching use-cases (PP).

    Returns: (events_sync, imp_in_types, pp_sorted)
    """
    # Subsets
    events = events_sync.copy()
    pp = skc_events.query("event_type == 'player_possession'").copy()
    obe_sorted = skc_events.query("event_type == 'on_ball_engagement'").sort_values(['frame_start']).copy()

    # Filter imp to actionable types for PP mapping
    prov_col_name = globals()[f'COL_TYPE_{provider}']
    prov_types_names = globals()[f'FIRST_TOUCH_{provider}'] + globals()[f'LAST_TOUCH_{provider}']

    events_to_match = events[events[prov_col_name].isin(prov_types_names)].copy()

    pp_sorted = pp.sort_values(['frame_start']).reset_index(drop=True)

    return events, events_to_match, pp_sorted, obe_sorted


def match_pressures_sb(
    sb_pressure: pd.DataFrame,
    obe_sorted: pd.DataFrame,
    pres_tol: int = PRESS_TOL_statsbomb,
    frame_name: str = 'frame_start',
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame_name not in obe_sorted.columns:
        msg = f'Colonne {frame_name} absente de obe_sorted'
        raise ValueError(msg)

    # SB -> SKC
    res_sb = []
    for pid, left_grp in sb_pressure.groupby('player_id'):
        right_grp = obe_sorted[obe_sorted['player_id'] == pid]
        if right_grp.empty:
            continue
        merged = pd.merge_asof(
            left=left_grp.sort_values('skc_frame'),
            right=right_grp.sort_values(frame_name),
            left_on='skc_frame',
            right_on=frame_name,
            direction='nearest',
            tolerance=pres_tol,
        )
        res_sb.append(merged)
    matched_pressure_sb = pd.concat(res_sb, ignore_index=True) if res_sb else pd.DataFrame()

    # SKC -> SB
    res_skc = []
    for pid, left_grp in obe_sorted.groupby('player_id'):
        right_grp = sb_pressure[sb_pressure['player_id'] == pid]
        if right_grp.empty:
            continue
        merged = pd.merge_asof(
            left=left_grp.sort_values(frame_name),
            right=right_grp.sort_values('skc_frame'),
            left_on=frame_name,
            right_on='skc_frame',
            direction='nearest',
            tolerance=pres_tol,
        )
        res_skc.append(merged)
    matched_pressure_skc = pd.concat(res_skc, ignore_index=True) if res_skc else pd.DataFrame()

    return matched_pressure_sb, matched_pressure_skc


def match_duels(  # noqa: C901, PLR0913, PLR0915
    events: pd.DataFrame,  # provider duels
    obe_sorted: pd.DataFrame,
    provider: str,
    duels_pp_used_ids: set,
    frame_used: str | None = None,  # col OBE for alignement
    tolerance: int | None = None,
) -> pd.DataFrame:
    """Match duels from various providers to SKC on-ball engagement events."""

    p = provider.lower().strip()

    # --- Config per provider ---
    skc_obe_pid = 'player_id_skc'
    skc_in_pos = 'player_in_possession_id'

    if p in ('wyscout', 'ws'):
        # Type: ground duel => aerialDuel_opponent_id NaN
        def type_mask(df: pd.DataFrame) -> pd.Series:
            return (df['type_primary'] == 'duel') & (df['aerialDuel_opponent_id'].isna())

        duels = events[type_mask(events) & ~events['id'].isin(duels_pp_used_ids)].copy()
        prov_pip = 'skc_groundDuel_opponent_id'
        prov_duel_pip = 'skc_player_id'

        config = {
            'col_frame_obe': frame_used or 'frame_end',
            'col_id_for_drop': 'event_id',
            'default_tol': globals().get('DUEL_TOL_wyscout', 40),
            'suffix_provider': '_ws',
            'suffix_obe': '_skc',
            'filter_required': {
                'type_primary',
                'aerialDuel_opponent_id',
                prov_pip,
                prov_duel_pip,
                skc_obe_pid,
                skc_in_pos,
            },
            'filter_fn': sym_duel_filter(type_mask, prov_pip, prov_duel_pip, skc_obe_pid, skc_in_pos),
        }

    elif p in ('impect', 'imp'):
        # Type: Ground Duel non Head
        def type_mask(df: pd.DataFrame) -> pd.Series:
            return (df['actionType'] == 'GROUND_DUEL') & (df['bodyPart'] != 'HEAD')

        duels = events[type_mask(events) & ~events['id'].isin(duels_pp_used_ids)].copy()
        prov_pip = 'skc_duel_playerId'
        prov_duel_pip = 'skc_player_id'

        config = {
            'col_frame_obe': frame_used or 'frame_start',
            'col_id_for_drop': 'event_id_skc',
            'default_tol': globals().get('DUEL_TOL_impect', 40),
            'suffix_provider': '_imp',
            'suffix_obe': '_skc',
            'filter_required': {'actionType', 'bodyPart', prov_pip, prov_duel_pip, skc_obe_pid, skc_in_pos},
            'filter_fn': sym_duel_filter(type_mask, prov_pip, prov_duel_pip, skc_obe_pid, skc_in_pos),
        }

    elif p in ('statsbomb', 'sb'):
        # Type: Duel non Aerial Lost
        def type_mask(df: pd.DataFrame) -> pd.Series:
            return (df['type_name'] == 'Duel') & (df['duel_type_name'] != 'Aerial Lost')

        duels = events[type_mask(events) & ~events['id'].isin(duels_pp_used_ids)].copy()
        prov_pip = 're_player_id'
        prov_duel_pip = 'player_id_sb'

        config = {
            'col_frame_obe': frame_used or 'frame_end',
            'col_id_for_drop': 'event_id',
            'default_tol': globals().get('DUEL_TOL_statsbomb', 40),
            'suffix_provider': '_sb',
            'suffix_obe': '_skc',
            'filter_required': {'type_name', 'duel_type_name', prov_pip, prov_duel_pip, skc_obe_pid, skc_in_pos},
            'filter_fn': sym_duel_filter(type_mask, prov_pip, prov_duel_pip, skc_obe_pid, skc_in_pos),
        }

    else:
        msg = 'provider must be "wyscout", "impect" or "sb"/"statsbomb".'
        raise ValueError(msg)

    col_fp = 'skc_frame'
    col_fo = config['col_frame_obe']
    tol = config['default_tol'] if tolerance is None else int(tolerance)
    suff_p = config['suffix_provider']
    suff_o = config['suffix_obe']
    col_drop = config['col_id_for_drop']
    need_cols = config['filter_required']
    filter_fn = config['filter_fn']

    type_provider_id = get_type_provider_id(provider)
    if 'id' in duels.columns:
        duels['id'] = duels['id'].apply(lambda x: type_provider_id(x) if pd.notna(x) else x)

    tmp = _bidir_merge_filter(
        duels,
        obe_sorted,
        col_fp=col_fp,
        col_fo=col_fo,
        tol=tol,
        suff_p=suff_p,
        suff_o=suff_o,
        col_drop=col_drop,
        need_cols=need_cols,
        filter_fn=filter_fn,
    )

    # ---------- Recast ID after merges/concat ----------
    if 'id' in tmp.columns:
        tmp['id'] = tmp['id'].apply(lambda x: type_provider_id(x) if pd.notna(x) else x)

    # ---------- Sets strongly typed ----------
    if 'id' in duels.columns:
        matched_ids = set[type_provider_id](tmp['id'].unique())
        total_ids = set[type_provider_id](duels['id'].unique())
        remaining_ids = total_ids - matched_ids

        duels_used_ids = set()
        if remaining_ids:
            duels_restants = duels[duels['id'].isin(remaining_ids)].copy()
            tmp_r = _bidir_merge_filter(
                duels_restants,
                obe_sorted,
                col_fp=col_fp,
                col_fo='frame_start',
                tol=tol,
                suff_p=suff_p,
                suff_o=suff_o,
                col_drop=col_drop,
                need_cols=need_cols,
                filter_fn=filter_fn,
            )
            if 'id' in tmp_r.columns:
                tmp_r['id'] = tmp_r['id'].apply(lambda x: type_provider_id(x) if pd.notna(x) else x)
            tmp = pd.concat([tmp, tmp_r], ignore_index=True)

            add_ids = set[type_provider_id](tmp_r['id'].unique())
            duels_used_ids = matched_ids.union(add_ids)
        else:
            duels_used_ids = matched_ids
    else:
        duels_used_ids = set()

    duels_used_ids = {type_provider_id(x) for x in duels_used_ids}
    return tmp, duels_used_ids


def apply_matching_duels(
    provider_events: pd.DataFrame, obe_sorted: pd.DataFrame, duels_pp_used_ids: set[Any], provider: str
) -> pd.DataFrame:
    col_duel_id = 'duel_playerId' if provider == 'impect' else 'groundDuel_opponent_id'

    map_player_ids = provider_events[['skc_player_id', 'player_id']].drop_duplicates()
    map_dict = dict(zip(map_player_ids['skc_player_id'], map_player_ids['player_id']))
    inv_map_dict = {v: k for k, v in map_dict.items()}

    provider_events[f'skc_{col_duel_id}'] = provider_events[col_duel_id].map(inv_map_dict)
    matched_duels, duels_used_ids = match_duels(provider_events, obe_sorted, provider, duels_pp_used_ids)

    return matched_duels.drop_duplicates(subset=['id'], keep='first'), duels_used_ids.union(duels_pp_used_ids)


def match_bundle(  # noqa: C901, PLR0912, PLR0915
    pp_row: pd.Series,
    events: pd.DataFrame,
    used_ids: set[Any],
    provider: str,
    duels_pp_used_ids: set[Any] | None = None,
) -> dict[str, Any]:
    """
    Match a player possession (pp_row) to events from a given provider (events),avoiding already used event IDs.
    Returns a dictionary with matching details including first touch, possessions, last touch, and pattern.
    Supported providers: "impect", "wyscout", "statsbomb".
    """

    # ----- Configuration per provider -----
    provider = provider.lower().strip()
    if provider not in {'impect', 'wyscout', 'statsbomb'}:
        msg = 'provider must be "impect", "wyscout" or "statsbomb"'
        raise ValueError(msg)

    col_pid = 'skc_player_id'
    col_frame = 'skc_frame'
    col_id = 'id'
    col_type = globals().get(f'COL_TYPE_{provider}', None)
    if provider == 'impect':
        last_touch_list = LAST_TOUCH_impect
        first_touch_list = FIRST_TOUCH_impect
        poss_list = LAST_TOUCH_impect
        duel_name = 'GROUND_DUEL'
    elif provider == 'wyscout':
        last_touch_list = LAST_TOUCH_wyscout
        first_touch_list = FIRST_TOUCH_wyscout
        poss_list = LAST_TOUCH_wyscout
        duel_name = 'duel'
    else:  # 'statsbomb'
        col_pid = 'player_id'
        last_touch_list = LAST_TOUCH_statsbomb
        first_touch_list = FIRST_TOUCH_statsbomb
        poss_list = POSSESSION_statsbomb
        duel_name = 'Duel'

    pid = pp_row['player_id']
    frame_start, frame_end = int(pp_row['frame_start']), int(pp_row['frame_end'])

    sub = events[(events[col_pid] == pid) & (events[col_frame].between(frame_start - PP_TOL, frame_end + PP_TOL))]

    # ----- Final -----
    lts = sub[sub[col_type].isin(last_touch_list)]
    lt = None
    if not lts.empty:
        # closest to frame_end
        cand_lt = lts.iloc[(lts[col_frame] - frame_end).abs().argsort()].iloc[0]
        if (cand_lt[col_id]) not in used_ids:
            lt = cand_lt
            used_ids.add(get_type_provider_id(provider)(lt[col_id]))
            if lt[col_type] == duel_name and duels_pp_used_ids is not None:
                duels_pp_used_ids.add(get_type_provider_id(provider)(lt[col_id]))

    # ----- FIRST_TOUCH -----
    fts = sub[sub[col_type].isin(first_touch_list)]
    ft = None
    if not fts.empty:
        # if last touch exists, only consider first touches before it
        fts_before_lt = fts if lt is None else fts[fts[col_frame] <= lt[col_frame]]
        if not fts_before_lt.empty:
            cand_ft = fts_before_lt.iloc[(fts_before_lt[col_frame] - frame_start).abs().argsort()].iloc[0]
            if (cand_ft[col_id]) not in used_ids:
                ft = cand_ft
                used_ids.add(get_type_provider_id(provider)(ft[col_id]))
                if ft[col_type] == duel_name and duels_pp_used_ids is not None:
                    duels_pp_used_ids.add(get_type_provider_id(provider)(ft[col_id]))
    # ----- Posssessions -----
    possessions_ids, possessions_frames, possessions_types = [], [], []
    possessions = sub[
        sub[col_type].isin(poss_list)
        & sub[col_frame].between(
            (int(ft[col_frame]) - 1 if ft is not None else frame_start - PP_TOL),
            (int(lt[col_frame]) if lt is not None else frame_end + PP_TOL),
        )
    ]
    for _, c in possessions.iterrows():
        cid = c[col_id]
        if cid not in used_ids:
            possessions_ids.append(cid)
            possessions_frames.append(int(c[col_frame]))
            possessions_types.append(c[col_type])
            used_ids.add(get_type_provider_id(provider)(cid))
            if c[col_type] == duel_name and duels_pp_used_ids is not None:
                duels_pp_used_ids.add(get_type_provider_id(provider)(cid))

    # ----- Pattern -----
    seq = []
    if ft is not None:
        seq.append(ft[col_type])
    if possessions_types:
        seq.extend(possessions_types)
    if lt is not None:
        seq.append(lt[col_type])
    pattern = '-'.join(seq) if seq else 'None'

    return {
        'event_id': pp_row['event_id'],
        'end_type': pp_row.get('end_type'),
        'frame_end': frame_end,
        'pattern': pattern,
        # FT
        'frame_FT': int(ft[col_frame]) if ft is not None else None,
        f'{provider}_FT_id': (ft[col_id]) if ft is not None else None,
        # Carries
        'frames_carry': possessions_frames,
        f'{provider}_possessions_ids': possessions_ids,
        # LAST TOUCH
        'frame_LT': int(lt[col_frame]) if lt is not None else None,
        f'{provider}_LT_id': (lt[col_id]) if lt is not None else None,
        'last_touch_type': lt[col_type] if lt is not None else None,
        # matching quality
        'delta_frame_end': (int(lt[col_frame]) - frame_end) if lt is not None else None,
        # Game interruption
        'gia': pp_row.get('game_interruption_after'),
        'gib': pp_row.get('game_interruption_before'),
    }


def _bidir_merge_filter(  # noqa: PLR0913
    duels: pd.DataFrame,
    obe_sorted: pd.DataFrame,
    *,
    col_fp: str,
    col_fo: str,
    tol: int,
    suff_p: str,
    suff_o: str,
    col_drop: str | None,
    need_cols: set[str],
    filter_fn: callable,
) -> pd.DataFrame:
    """merge_asof bidirectionnel + concat + drop dup + filtre optionnal"""
    duels[col_fp] = duels[col_fp].astype('int64', errors='ignore')
    obe_sorted[col_fo] = obe_sorted[col_fo].astype('int64', errors='ignore')

    m0 = pd.merge_asof(
        duels.sort_values(col_fp),
        obe_sorted.sort_values(col_fo),
        left_on=col_fp,
        right_on=col_fo,
        direction='nearest',
        tolerance=tol,
        suffixes=(suff_p, suff_o),
    )
    m1 = pd.merge_asof(
        obe_sorted.sort_values(col_fo),
        duels.sort_values(col_fp),
        left_on=col_fo,
        right_on=col_fp,
        direction='nearest',
        tolerance=tol,
        suffixes=(suff_o, suff_p),
    )
    out = pd.concat([m0, m1], ignore_index=True)

    if col_drop and (col_drop in out.columns):
        out = out.drop_duplicates(subset=[col_drop], keep='first')

    if need_cols.issubset(out.columns):
        out = filter_fn(out)

    return out


def sym_duel_filter(
    type_mask_fn: callable, prov_pip: str, prov_duel_pip: str, skc_obe_pid: str, skc_in_pos: str
) -> callable:
    """Construct symmetric duel filter function."""

    def _f(df: pd.DataFrame) -> pd.DataFrame:
        mask_type = type_mask_fn(df)
        return df[mask_type & ((df[prov_duel_pip] == df[skc_obe_pid]) & (df[prov_pip] == df[skc_in_pos]))]

    return _f
