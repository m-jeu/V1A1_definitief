from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import statistics


def create_product_attribute_and_price_information_table(db: PostgresDAO.db, product_attribute: str = "sub_sub_category"):
    """Create a PostgreSQL table to contain price information about a certain product attribute
    called {product_attribute}_price_information.

    The table will have 3 rows:
    -a row for the value of the product attribute, with the name of the product attribute as primary key.
    -a row for the average price of products with this product attribute value called average_price.
    -a row for the standard deviation of products with this product attribute value called standard_deviation.

    Args:
        db: the postgreSQL database to query.
        product_attribute: what the product attribute is called. 'sub_sub_category' by default."""
    db.query(f"DROP TABLE IF EXISTS {product_attribute}_price_information;", commit_changes=True)
    query = f"""CREATE TABLE {product_attribute}_price_information
(
    {product_attribute} VARCHAR,
    average_price FLOAT,
    standard_deviation FLOAT,
    PRIMARY KEY({product_attribute});"""
    db.query(query, commit_changes=True)


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

    Args:
        grouped_attribute_prices: the return from group_attribute_prices()

    Returns:
        dataset in the format of [(attribute_value, price_average, price_standard_deviation)]"""
    dataset = []
    for attribute_value, prices in grouped_attribute_prices.items():
        price_avg = statistics.avg(prices)
        standard_deviation = statistics.standard_deviation(prices, price_avg)
        dataset.append((attribute_value, price_avg, standard_deviation))
    return dataset


