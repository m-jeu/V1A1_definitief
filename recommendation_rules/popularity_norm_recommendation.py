from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions
import datetime


WEEKINSECONDS = 7*24*60*60
DATASET_START, DATASET_END = PostgresDAO.db.query("SELECT min(session_end), max(session_end) FROM Sessions;", expect_return=True)[0]
TODAY = DATASET_END


class ChronologyError(Exception):
    """A custom exception to help diagnose problems with the time_travel function."""
    def __init__(self, dataset_start: datetime.datetime, dataset_end: datetime.datetime):
        super().__init__(f"""You can't set the date earlier or later then the dataset,
It has to between {dataset_start.strftime('%d-%m-%Y')} and {dataset_end.strftime('%d-%m-%Y')}.""")


#TODO: write documentation, add some print statements

#TODO: Consider adding to Product class
def score_multiplier(x: int, sensitivity: float = 1.0, upper_bound: float = 3.0) -> float:
    """A multiplier to cancel out the disproportional relative increases in sales
    when a product gets bought that doesn't get bought often.
    Discriminates against products up to a certain amount of weekly sales

    Args:
        x: the average amount of weekly sales a product has.
        sensitivity:
            1 by default. If set higher, multiplier discriminates against products with a higher
            amount of weekly purchases.
        upper_bound:
            should be set to the solution to (x^2) / (9 * multiplier = 1).

    Returns:
        number between 0 and 1 to multiply the relative increase in weekly sales by"""
    if x > upper_bound:
        return float(1)
    return (x ** 2) / (9 * sensitivity)


class Product:
    """Class to store the sales figures of a single product-id.
    Stores itself as value in the tracker, with it's product_id as key.

    Attributes:
        tracker:
            universal dictionairy through all class instances that stores all class instances as values,
            with their product_ids as keys.
        product_id(str): a product's ID as it appears in MongoDB/PostgreSQL.
        today(datetime.datetime): the date/time the recommendation thinks it is.
        sold(int): the total amount of times this product has been ordered.
        sold_lastweek(int): the amount of times this product was sold a week before self.today.
        earliest_order(datetime.datetime): the earliest day/time this product was found to be sold at.
        average_weekly_sales: the average amount of times this product is sold per week.
        score: this product's recommendibility score as determined by the algorithm.
        """
    tracker = {}
    def __init__(self, product_id: str, today: datetime.datetime):
        """Initialize class instance.

        Args:
            product_id: this product's product_id as it appears in MongoDB/PostgreSQL.
            today: the date/time the algorithm thinks it is.

        Also adds itself to Product.tracker as value, with product_id as key.
            """
        self.product_id = product_id
        self.today = today
        self.sold = 0
        self.sold_lastweek = 0
        self.earliest_order = today
        self.average_weekly_sales = None
        self.score = None
        Product.tracker[product_id] = self

    def add_order(self, order_tuple: tuple[str, int, datetime.datetime]):
        """Add an order as it's returned by the SQL query in Product.get_from_pg()
        to this product's sold, sold_lastweek and earliest_order.

        Args:
            order_tuple: a tuple from the SQL query in Product.get_from_pg()."""
        product_id, quantity, session_end = order_tuple
        self.sold += quantity
        if (self.today - session_end).total_seconds() < WEEKINSECONDS:
            self.sold_lastweek += quantity
        if session_end < self.earliest_order:
            self.earliest_order = session_end

    def avg_sales_per_week(self):
        """Calculate this product's average sales per week, and assign it to self.average_weekly_sales."""
        timespan = self.today - self.earliest_order
        week_amount = timespan.total_seconds() / WEEKINSECONDS
        self.average_weekly_sales = self.sold / week_amount

    def calc_score(self) -> float:
        """Calculate this product's recommendibility-score.
        Call self.avg_sales_per_week() and score_multiplier to calculate it..

        Assigns result to self.score, and returns it.

        Returns:
            this product's recommendibility_score"""
        self.avg_sales_per_week()
        increase_from_norm = self.sold_lastweek / self.average_weekly_sales
        score = increase_from_norm * score_multiplier(self.average_weekly_sales)
        self.score = score
        return self.score

    def __str__(self):
        return f"Product with product_id {self.product_id}"

    def __repr__(self):
        return self.__str__()

    def __gt__(self, product2):
        return self.score > product2.score

    @staticmethod
    def get_from_pg(db: PostgresDAO.PostgreSQLdb, up_until_date: datetime.datetime = None):
        """Get all ordered_products from PostgreSQL, and assign them to / create the proper Product instances.

        Args:
            db: the PostgreSQL database to query for ordered_products.
            up_until_date: the date up until which session should be included (for time-travelling purposes)."""
        query = """SELECT Ordered_products.product_id, Ordered_products.quantity, Sessions.session_end
            FROM SESSIONS
            INNER JOIN Ordered_products ON Sessions.session_id = Ordered_products.session_id
            """

        if not up_until_date is None:
            query += f"WHERE Sessions.session_end < '{up_until_date.strftime('%Y-%m-%d')}'"

        query += ";"

        order_dataset = db.query(query, expect_return=True)

        for result in order_dataset:
            if not result[0] in Product.tracker:
                Product(result[0], TODAY)
            Product.tracker[result[0]].add_order(result)

    @staticmethod
    def score_all():
        """Iterate through all Product instances in Product.tracker, and call calc_score() on them."""
        for product in Product.tracker.values():
            product.calc_score()


#TODO: Consider changing name to leaderboard.
#TODO: Finish docstrings.
class Top:
    """A class to allow easy GREATER_THEN comparisons, and collect the top N objects in a dataset."""
    def __init__(self, length: int):
        self.length = length
        self.data = []

    def insert(self, object):
        if self.data == []:
            self.data.append(object)
        else:
            for i in range(len(self.data) - 1, -2, -1):
                if i == -1:
                    self.data.insert(0, object)
                elif not object > self.data[i]:
                    self.data.insert(i + 1, object)
                    break
        if len(self.data) > self.length:
            self.data.pop()#TODO: Rewrite so object doesn't get added at index over self.length - 1 at all instead of popping it

    def insert_multiple(self, object_list: list):
        for object in object_list:
            self.insert(object)


def time_travel(day: int, month: int, year: int, dataset_start: datetime.datetime, dataset_end: datetime.datetime):
    """Change the global TODAY constant to a different date, so that the recommendation algorithm can be tested
    by pretending it's different historical days.

    Args:
        day: the day of the month to time travel to.
        month: the month of the year to time travel to.
        year: the year to time travel to.
        dataset_start: the first date/time in the dataset.
        dataset_end: the last date/time in the dataset.

    Raises:
        ChronologyError if new date is not within dataset start/end."""
    global TODAY
    new_date = datetime.datetime(year, month, day)
    if new_date < dataset_start or new_date > dataset_end:
        raise ChronologyError(dataset_start, dataset_end)
    TODAY = new_date


def popularity_recommendation(current_date: datetime.datetime, db: PostgresDAO.PostgreSQLdb):
    """Call all necessary recommendations to fill the popularity_recommendation table in PostgreSQL,
    according to provided settings.

    Args:
        current_date, the date / time the algorhitm should think it is.
        db: the PostgreSQL DB to query."""
    Product.get_from_pg(db, current_date)
    Product.score_all()
    top = Top(4)
    top.insert_multiple(Product.tracker.values())
    insert_dataset = [tuple([product.product_id for product in top.data])]
    query_functions.create_rec_table_query(PostgresDAO.db, 'popularity_recommendation', '')
    db.query("INSERT INTO popularity_recommendation VALUES %s", insert_dataset, commit_changes=True)


def start_popularity_norm_recommendation():
    print("--START popularity_norm_recommendation--", end="\n\n\n")
    print("Creating popularity based recommendation.")
    time_travel(22, 7, 2018, DATASET_START, DATASET_END)
    popularity_recommendation(TODAY, PostgresDAO.db)
    print("Finished popularity based recommendation.")


def start_time_travel(day, month, year):
    print("--START popularity_norm_recommendation--", end="\n\n\n")
    print("Creating popularity based recommendation.")
    time_travel(day, month, year, DATASET_START, DATASET_END)
    popularity_recommendation(TODAY, PostgresDAO.db)
    print(f"traveld through time to: {day}, {month}, {year}")
    print("Finished popularity based recommendation.")


if __name__ == "__main__":
    start_popularity_norm_recommendation()