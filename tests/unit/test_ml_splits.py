import datetime as dt

import pytest

from lakehouse_platform.ml.splits import (
    Split,
    assert_embargo,
    rolling_origin_splits,
    time_split,
)

START = dt.date(2023, 1, 1)
END = dt.date(2023, 12, 31)


def test_holdout_puts_the_test_window_at_the_end_of_history():
    split = time_split(START, END, test_days=28, horizon_days=14)
    assert split.test_end == END
    assert split.test_days == 28
    assert split.train_start == START


def test_embargo_defaults_to_the_forecast_horizon():
    split = time_split(START, END, test_days=28, horizon_days=14)
    assert split.embargo_days == 14
    # The embargoed days belong to neither window.
    boundary = split.train_end + dt.timedelta(days=7)
    assert not split.contains_train(boundary)
    assert not split.contains_test(boundary)


def test_embargo_can_be_set_to_zero_deliberately():
    split = time_split(START, END, test_days=28, horizon_days=14, embargo_days=0)
    assert split.embargo_days == 0
    assert split.test_start == split.train_end + dt.timedelta(days=1)


def test_a_wider_embargo_costs_training_days():
    narrow = time_split(START, END, test_days=28, horizon_days=14, embargo_days=0)
    wide = time_split(START, END, test_days=28, horizon_days=14, embargo_days=28)
    assert wide.train_days == narrow.train_days - 28


def test_history_too_short_for_the_requested_split_is_an_error():
    with pytest.raises(ValueError, match="too short"):
        time_split(START, START + dt.timedelta(days=10), test_days=28, horizon_days=14)


def test_overlapping_windows_cannot_be_constructed():
    with pytest.raises(ValueError, match="overlaps"):
        Split(
            name="bad",
            train_start=START,
            train_end=dt.date(2023, 6, 30),
            test_start=dt.date(2023, 6, 1),
            test_end=dt.date(2023, 7, 31),
        )


def test_rolling_origin_walks_the_forecast_origin_forward():
    folds = rolling_origin_splits(START, END, test_days=28, horizon_days=14, folds=4)
    assert [split.name for split in folds] == ["fold_1", "fold_2", "fold_3", "fold_4"]

    # Oldest first, each origin one test window later, last fold ending at END.
    starts = [split.test_start for split in folds]
    assert starts == sorted(starts)
    assert folds[-1].test_end == END
    assert (folds[1].test_start - folds[0].test_start).days == 28


def test_rolling_origin_test_windows_do_not_overlap_at_the_default_step():
    folds = rolling_origin_splits(START, END, test_days=28, horizon_days=14, folds=4)
    for earlier, later in zip(folds, folds[1:]):
        assert earlier.test_end < later.test_start


def test_expanding_folds_grow_and_sliding_folds_do_not():
    expanding = rolling_origin_splits(START, END, test_days=28, horizon_days=14, folds=3)
    sliding = rolling_origin_splits(
        START, END, test_days=28, horizon_days=14, folds=3, expanding=False
    )

    assert [split.train_days for split in expanding] == sorted(
        split.train_days for split in expanding
    )
    assert len({split.train_days for split in expanding}) == 3
    assert len({split.train_days for split in sliding}) == 1


def test_sliding_folds_still_end_where_their_expanding_twin_ends():
    expanding = rolling_origin_splits(START, END, test_days=28, horizon_days=14, folds=3)
    sliding = rolling_origin_splits(
        START, END, test_days=28, horizon_days=14, folds=3, expanding=False
    )
    assert [s.train_end for s in sliding] == [s.train_end for s in expanding]
    assert [s.test_start for s in sliding] == [s.test_start for s in expanding]


def test_assert_embargo_accepts_folds_that_clear_the_horizon():
    folds = rolling_origin_splits(START, END, test_days=28, horizon_days=14, folds=3)
    assert_embargo(folds, minimum_days=14)


def test_assert_embargo_catches_a_rolling_window_wider_than_the_gap():
    """A 28-day rolling feature read across a 14-day embargo is the bug."""
    folds = rolling_origin_splits(START, END, test_days=28, horizon_days=14, folds=3)
    with pytest.raises(ValueError, match="embargo narrower"):
        assert_embargo(folds, minimum_days=28)
