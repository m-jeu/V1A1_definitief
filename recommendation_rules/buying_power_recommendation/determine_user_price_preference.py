from V1A1_definitief.database import PostgresDAO


def get_attribute_price_information(db: PostgresDAO.PostgreSQLdb, product_attribute: str) -> dict:
    """Query the PostgreSQL for price information about a certain product attribute
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


def profile_order_products_from_postgreSQL(db: PostgresDAO.PostgreSQLdb, product_attribute: str) -> list[tuple]:
    """Query the PostgreSQL database for the profile_id, a certain product attribute, the quantity, and the selling
    price for every product that has been ordered by a profile.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product attribute to get for every ordered product (for instance, 'sub_sub_category').

    Returns:
        a list containing a tuple for every product ordered in a session by a profile, that contains:
            0: the profile_id associated with the order.
            1: the value of the given product attribute.
            2: the amount of times that product was ordered in that session.
            3: the product's price in cents."""
    query = f"""SELECT
	Profiles.profile_id,
	Products.{product_attribute},
	Ordered_products.quantity,
	Products.selling_price
FROM
	Profiles
INNER JOIN
	Bu
	ON Profiles.profile_id = Bu.profile_id
INNER JOIN
	Sessions
	ON Bu.bu_id = Sessions.bu_id
INNER JOIN
	Ordered_products
	ON Sessions.session_id = Ordered_products.session_id
INNER JOIN
	Products
	ON Ordered_products.product_id = Products.product_id
;"""
    return db.query(query, expect_return=True)



