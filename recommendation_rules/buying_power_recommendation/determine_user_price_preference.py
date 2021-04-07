from V1A1_definitief.database import PostgresDAO


def get_attribute_price_information(db: PostgresDAO.PostgreSQLdb, product_attribute: str) -> dict:
    """Query a PostgreSQL for price information about a certain product attribute
    as entered into the PostgreSQL database by subsubcat_price_information.py.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product attribute to get price information about (for instance, 'sub_sub_category').

    Returns:
        a dictionairy with every value of the provided product attribute as keys,
        and a tuple containing:
            0: the average price of products with that value of the given product attribute.
            1: the price standard deviation of products with that value of the given product attribute.
        as values.
        """
    price_information = {}
    query_result = db.query(f"""SELECT {product_attribute}, average_price, standard_deviation
FROM {product_attribute}_price_information;""", expect_return=True)
    for att_value, avg, dev in query_result:
        price_information[att_value] = (avg, dev)
    return price_information



