from V1A1_definitief.database import PostgresDAO


def get_attribute_price_information(db: PostgresDAO.db, product_attribute: str = "sub_sub_category") -> list[tuple]:
    """Query PostgreSQL database (as configured by mongo_to_pg.py) for a certain attribute and
    the selling price of every product in the Products table.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product_attribute to get (sub_sub_category by default)."""
    query = f"SELECT {product_attribute}, selling_price FROM Products;"
    return db.query(query, expect_return=True)


