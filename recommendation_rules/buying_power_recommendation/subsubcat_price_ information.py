from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import statistics


def get_attribute_price_information(db: PostgresDAO.db, product_attribute: str = "sub_sub_category") -> list[tuple]:
    """Query PostgreSQL database (as configured by mongo_to_pg.py) for a certain attribute and
    the selling price of every product in the Products table.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product_attribute to get (sub_sub_category by default).

    Returns:
        a list containing a tuple containing: (product_attribute, price) for every product."""
    query = f"SELECT {product_attribute}, selling_price FROM Products;"
    return db.query(query, expect_return=True)


def group_attribute_prices(dataset: list[tuple]) -> dict:
    """Group the dataset as retrieved by get_attribute_price_information() into all prices belonging to a single
    product_attribute value.

    Args:
        dataset: the dataset as returned by get_attribute_price_information()

    Returns:
        a dictionairy containing every value of the earlier specified product attribute as keys,
        and all prices associated with that product attribute value contained within a list as value."""
    grouped_dict = {}
    for attribute, price in dataset:
        if attribute in grouped_dict:
            grouped_dict[attribute].append(price)
        else:
            grouped_dict[attribute] = [price]
    return grouped_dict


def grouped_attribute_prices_to_PostgreSQL_dataset(grouped_attribute_prices: dict) -> list[tuple]:
    """Convert the return from group_attribute_prices into a dataset that can be used to insert into a PostgreSQL
    database in the format of [(attribute_value, price_average, price_standard_deviation)].

    """
    dataset = []
    for attribute_value, prices in grouped_attribute_prices.items():
        price_avg = statistics.avg(prices)
        standard_deviation = statistics.standard_deviation(prices, price_avg)
        dataset.append((attribute_value, price_avg, standard_deviation))
    return dataset