def calculate(start_price: float, end_price: float):
    """
    Calculate the absolute ratio of change considering a single start and end price
    :param start_price: float
    :param end_price: float
    :return: float
    """

    if start_price == 0:
        return float("inf")

    return (end_price - start_price) / start_price
