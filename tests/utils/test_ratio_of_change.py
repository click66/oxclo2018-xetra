from xetra_analyser.utils.ratio_of_change import calculate


def test_calculate_ratio_of_change():
    for data in [
        (1, 1, 0),
        (1, 2, 1),
        (12, 16, 1/3),
        (16, 14, -0.125),
        (1, 999, 998),
        (1, 0, -1),
    ]:
        start_price = data[0]
        end_price = data[1]
        expected = data[2]

        assert calculate(start_price, end_price) == expected


def test_result_is_infinity_if_start_price_is_zero():
    for end_price in [-9999, -1, 0, 1, 9999, 999999999]:
        assert calculate(0, end_price) == float("inf")
