from data_processor.processor import find_scores


def test_find_scores():
    input = [{"gamesA": 3, "gamesB": 6}, {"gamesA": 2, "gamesB": 6}]
    expected_output = {"A": 0, "B": 2}
    output = find_scores(sets_score=input)
    assert expected_output == output
