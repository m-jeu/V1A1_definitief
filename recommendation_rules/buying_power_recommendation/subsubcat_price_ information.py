from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import statistics
from V1A1_definitief.recommendation_rules import query_functions


def create_product_attribute_and_price_information_table(db: PostgresDAO.db, product_attribute: str):
    """Create a PostgreSQL table to contain price information about a certain product attribute
    called {product_attribute}_price_information.

    The table will have 3 rows:
    -a row for the value of the product attribute, with the name of the product attribute as primary key.
    -a row for the average price of products with this product attribute value called average_price.
    -a row for the standard deviation of products with this product attribute value called standard_deviation.

    Args:
        db: the postgreSQL database to query.
        product_attribute: what the product attribute is called. For instance, 'sub_sub_category'."""
    db.query(f"DROP TABLE IF EXISTS {product_attribute}_price_information;", commit_changes=True)
    query = f"""CREATE TABLE {product_attribute}_price_information
(
    {product_attribute} VARCHAR,
    average_price FLOAT,
    standard_deviation FLOAT,
    PRIMARY KEY({product_attribute})
);"""
    db.query(query, commit_changes=True)


def get_attribute_price_information(db: PostgresDAO.db, product_attribute: str) -> list[tuple]:
    """Query PostgreSQL database (as configured by mongo_to_pg.py) for a certain attribute and
    the selling price of every product in the Products table.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product_attribute to get. For instance, 'sub_sub_category'.

    Returns:
        a list containing a tuple containing: (product_attribute, price) for every product."""
    query = f"SELECT {product_attribute}, selling_price FROM Products;"
    return db.query(query, expect_return=True)


def group_attribute_prices(dataset: list[tuple]) -> dict:
    """Group the dataset as retrieved by get_attribute_price_information() into all prices belonging to a single
    product_attribute value.

    Drops any entries where the product attribute value is None.

    Args:
        dataset: the dataset as returned by get_attribute_price_information()

    Returns:
        a dictionairy containing every value of the earlier specified product attribute as keys,
        and all prices associated with that product attribute value contained within a list as value."""
    grouped_dict = {}
    for attribute, price in dataset:
        if not attribute is None:
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


def price_info_in_pg(db: PostgresDAO.PostgreSQLdb, product_attribute: str):
    """Create and fill a table called {product_attribute}_price_information in PostgreSQL
    with information about the average price and standard price deviation of products with a certain value of a certain
    product_attribute.

    PostgreSQL table will be called {product_attribute}_price_information.

    The table will have 3 rows:
    -a row for the value of the product attribute, with the name of the product attribute as primary key.
    -a row for the average price of products with this product attribute value called average_price.
    -a row for the standard deviation of products with this product attribute value called standard_deviation.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product attribute to get product information by. For instance, 'sub_sub_category'."""
    create_product_attribute_and_price_information_table(db, product_attribute)

    price_information = get_attribute_price_information(db, product_attribute)

    grouped_price_information = group_attribute_prices(price_information)

    dataset = grouped_attribute_prices_to_PostgreSQL_dataset(grouped_price_information)

    query = query_functions.construct_insert_query(f"{product_attribute}_price_information",
                                                    [product_attribute, "average_price", "standard_deviation"])

    db.many_update_queries(query, dataset, fast_execution=True)

#TODO: Consider not adding to PostgreSQL, and passing to determine_user_price_preference.py directly instead.
if __name__ == "__main__":
    price_info_in_pg(PostgresDAO.db, "sub_sub_category")