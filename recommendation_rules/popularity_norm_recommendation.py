from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions
import datetime


WEEKINSECONDS = 7*24*60*60
TODAY = PostgresDAO.db.query("SELECT sessions.session_end FROM sessions order by session_end DESC LIMIT 1;", expect_return=True)[0][0]
#TODAY -= datetime.timedelta(days=60)


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
    tracker = {}
    def __init__(self, product_id: str, today: datetime.datetime):
        self.product_id = product_id
        self.start_date = None
        self.today = today
        self.sold = 0
        self.sold_lastweek = 0
        self.earliest_order = today
        self.average_weekly_sales = None
        self.score = None
        Product.tracker[product_id] = self

    def add_order(self, order_tuple: tuple[str, int, datetime.datetime]):
        product_id, quantity, session_end = order_tuple
        self.sold += quantity
        if (self.today - session_end).total_seconds() < WEEKINSECONDS:
            self.sold_lastweek += quantity
        if session_end < self.earliest_order:
            self.earliest_order = session_end

    def avg_sales_per_week(self):
        timespan = self.today - self.earliest_order
        week_amount = timespan.total_seconds() / WEEKINSECONDS
        self.average_weekly_sales = self.sold / week_amount

    def score(self) -> int:
        self.avg_sales_per_week()
        increase_from_norm = self.sold_lastweek / self.average_weekly_sales
        score = increase_from_norm * score_multiplier(self.average_weekly_sales)
        self.score = score
        return self.score

    def __str__(self):
        return f"Product with product_id {self.product_id}"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get_from_pg(db: PostgresDAO.PostgreSQLdb):
        query = """SELECT Ordered_products.product_id, Ordered_products.quantity, Sessions.session_end
            FROM SESSIONS
            INNER JOIN Ordered_products ON Sessions.session_id = Ordered_products.session_id
            ;"""

        order_dataset = PostgresDAO.db.query(query, expect_return=True)

        for result in order_dataset:
            if not result[0] in Product.tracker:
                Product(result[0], TODAY)
            Product.tracker[result[0]].add_order(result)
