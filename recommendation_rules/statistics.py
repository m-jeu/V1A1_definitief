def avg(data: list[float or int]) -> float:
    """Calculate the average from list of numbers.

    Args:
        data: list of numbers.

    Returns:
        the average."""
    return sum(data) / len(data)


def standard_deviation(data: list[float or int], average: float = None) -> float:
    """Calculate the standard deviation from list of numbers.

    Args:
        data: list of numbers.
        average: the average from the numbers list. If unknown, leave empty.

    Returns:
        the standard deviation."""
    if average is None:
        average = avg(data)
    variance = avg([abs(x - average) ** 2 for x in data])
    return variance ** (1/2)

