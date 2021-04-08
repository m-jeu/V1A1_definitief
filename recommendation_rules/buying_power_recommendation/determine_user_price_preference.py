from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import statistics


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


class OrderedProduct:
    """A product, as ordered by a certain profile_id, in 1 session.

    Attributes:
        profile_id: the profile ID the product was ordered by.
        attribute_value: the value of a certain product attribute, as specified in profile_order_products_from_postgreSQL().
        quantity: the amount of times this product was ordered by this profile in that session.
        price: the product's price in cents.
        devs_from_avg:
            the amount of standard deviations (positive or negative) this product is removed from the average
            of it's product attribute value (as provided by get_attribute_price_information)."""
    def __init__(self, order_tuple: tuple, att_price_info: dict): #TODO: Check what attributes actually need to be saved
        """Initialize class instance.

        Args:
            order_tuple: a tuple as returned in list by profile_order_products_from_postgreSQL().
            att_price_info:
                a dictionairy containing the average price and standard price deviation for products with a certain
                attribute_value, as returned by get_attribute_price_information()"""
        self.profile_id, self.attribute_value, self.quantity, self.price = order_tuple
        self.devs_from_avg = ((att_price_info[self.attribute_value][0] - self.price) / att_price_info[self.attribute_value][1])


class Profile:
    """A profile.

    Attributes:
        id: a profile's id.
        ordered_products: list containing an OrderedProduct object for every product ordered in a single session.
        budget_segment:
            'Normal', 'Budget' or 'Luxury' to represent this profile's spending habits, None if not enough
            data is available to draw conclusions from.
        tracker: a class-wide dictionairy that contains profile_ids as keys, and Profile objects as values."""
    tracker = {}
    def __init__(self, id: str):
        """Initialize class instance, and store self in Profile.tracker as value under own profile_id as key.

        Args:
            id: profile_id"""
        self.id = id
        self.ordered_products = []
        self.budget_segment = None
        Profile.tracker[id] = self

    def insert_product(self, ordered_product: OrderedProduct):
        """Insert an ordered_product into self.ordered_products.

        Args:
            ordered_product: The OrderedProduct object to insert."""
        self.ordered_products.append(ordered_product)

    def calculate_budget_segment(self, deviation_amount: int = 1):
        """Calculate what budget segment this profile is in, and assign it to self.budget_segment.
        If there are no products in self.ordered_products, self.budget_segment stays at None.

        Args:
            deviation_amount:
                the average amount of standard price deviations a profile should vary from the average
                in whatever sub_sub_categories they have bought products you should start drawing conclusions from.
                1 by default."""
        if len(self.ordered_products) > 0:
            deviations = []
            for o_p in self.ordered_products:
                deviations += [o_p.devs_from_avg] * o_p.quantity
            avg_deviation = statistics.avg(deviations)
            if avg_deviation > deviation_amount:
                self.budget_segment = "Luxury"
            elif avg_deviation < -deviation_amount:
                self.budget_segment = "Budget"
            else:
                self.budget_segment = "Normal"

    @staticmethod
    def calculate_budget_segment_ALL(deviation_amount: int = 1):
        """Call calculate_budget_segment() on all Profiles in Profile.tracker.

        Args:
            the average amount of standard price deviations a profile should vary from the average
            in whatever sub_sub_categories they have bought products you should start drawing conclusions from.
            1 by default."""
        for profile in Profile.tracker.values():
            profile.calculate_budget_segment(deviation_amount)

    @staticmethod
    def get_all_from_pg(db: PostgresDAO.PostgreSQLdb, product_attribute: str):
        """Call get_attribute_price_information() and profile_order_products_from_postgreSQL to get all
        necesary information to add all products by a profile with the relevant attributes to Profile.tracker.

        Args:
            db: The PostgreSQL database to query.
            product_attribute:
                the product attribute to use to calculate price preference tendencies, for instance, 'sub_sub_category'.
                NOTE: This product attribute needs to be used in subsubcat_price_information.py first."""
        price_information = get_attribute_price_information(db, product_attribute)
        dataset = profile_order_products_from_postgreSQL(db, product_attribute)
        for tuple in dataset:
            if not tuple[0] in Profile.tracker:
                Profile(tuple[0])
            if not tuple[1] is None: #If the product_attribute_value is None, adding an ordered_product is quite pointless.
                if price_information[tuple[1]][1] != 0.0: #If a product_attribute_value's standard deviation, adding the ordered product is also quite pointless.
                    Profile.tracker[tuple[0]].insert_product(OrderedProduct(tuple, price_information))

    @staticmethod
    def write_all_to_pg(db: PostgresDAO.PostgreSQLdb):
        """Write all profiles in Profile.tracker to the corresponding entry in the Profiles table in PostgreSQL.

        Args:
            db: The PostgreSQL database to write to."""
        dataset = []
        for profile in Profile.tracker.values():
            dataset.append((profile.budget_segment, profile.id))
        query = "UPDATE Profiles SET budget_preference = %s WHERE profile_id = %s;"
        db.many_update_queries(query, dataset, fast_execution=True)


def determine_user_price_preferences(db: PostgresDAO.PostgreSQLdb, product_attribute: str):
    """Call all necessary methods to gather data about price preference, process it, and write it to PostgreSQL.

    Args:
        db: The PostgreSQL database to query.
        product_attribute: the product attribute budget preference should be judged by."""
    Profile.get_all_from_pg(db, product_attribute)
    Profile.calculate_budget_segment_ALL()
    Profile.write_all_to_pg(db)


if __name__ == "__main__":
    determine_user_price_preferences(PostgresDAO.db, "sub_sub_category")