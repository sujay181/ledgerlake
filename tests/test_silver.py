from ledgerlake import silver


def test_merge_condition_composite_keys():
    assert silver.merge_condition(("a", "b")) == "t.`a` = s.`a` AND t.`b` = s.`b`"
    assert silver.merge_condition(["id"]) == "t.`id` = s.`id`"
