from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions
import datetime

WEEKINSECONDS = 7*24*60*60


class Product:
    tracker = {}
    def __init__(self, product_id: str, today: datetime.datetime):
        self.product_id = product_id
        self.start_date = None
        self.today = today
        self.sold = 0
        self.sold_lastweek = 0
        self.earliest_order = today
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
        week_amount = timespan / WEEKINSECONDS
        self.average_sales = (self.sold / week_amount)

    def check_validity(self, min_average: int = 5):
        return (self.average_sales < min_average)

    def __str__(self):
        return f"Product with product_id {self.product_id}"

    def __repr__(self):
        return self.__str__()


if __name__ == "__main__":
    query = """SELECT Ordered_products.product_id, Ordered_products.quantity, Sessions.session_end
    FROM SESSIONS
    INNER JOIN Ordered_products ON Sessions.session_id = Ordered_products.session_id
    ;"""

    order_dataset = PostgresDAO.db.query(query, expect_return=True)

    for result in order_dataset:
        if not result[0] in Product.tracker:
            Product(result[0], datetime.datetime.today())
        Product.tracker[result[0]].add_order(result)