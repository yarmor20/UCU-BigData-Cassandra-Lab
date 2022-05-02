from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from utils import split_date, next_weekday
import json


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        return self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_product_reviews(self, table, review_id, product_id, review_headline, review_body, review_date):
        query = f"""INSERT INTO {table} (review_id, product_id, review_headline, review_body, review_date)
        VALUES ('{review_id}', '{product_id}', '{review_headline}', '{review_body}', '{review_date}');
        """
        self.execute(query)

    def insert_rated_reviews(self, table, review_id, product_id, star_rating, review_headline, review_body, review_date):
        query = f"""INSERT INTO {table} (review_id, product_id, star_rating, review_headline, review_body, review_date)
        VALUES ('{review_id}', '{product_id}', {star_rating}, '{review_headline}', '{review_body}', '{review_date}');
        """
        self.execute(query)

    def insert_customer_reviews(self, table, customer_id, review_id, product_id, review_headline, review_body, review_date):
        query = f"""INSERT INTO {table} (customer_id, review_id, product_id, review_headline, review_body, review_date)
        VALUES ('{customer_id}', '{review_id}', '{product_id}', '{review_headline}', '{review_body}', '{review_date}');
        """
        self.execute(query)

    def insert_unique_products(self, table, product_id, product_title):
        product_exists = list(self.execute(f"SELECT * FROM {table} WHERE product_id = '{product_id}';"))

        if not product_exists:
            query = f"""INSERT INTO {table} (product_id, product_title)
            VALUES ('{product_id}', '{product_title}');
            """
            self.execute(query)

    def insert_review_daily(self, table, product_id, product_title, review_date):
        query = f"""
        SELECT review_count 
        FROM {table}
        WHERE product_id = '{product_id}' AND review_date = '{review_date}';
        """
        current_review_count = self.execute(query)

        review_count = 1
        if not current_review_count:
            query = f"""
            INSERT INTO {table} (product_id, product_title, review_count, review_date)
            VALUES ('{product_id}', '{product_title}', {review_count}, '{review_date}');
            """
        else:
            review_count = list(current_review_count)[0][0]
            query = f"""
            UPDATE {table}
            SET review_count = {review_count + 1}
            WHERE product_id = '{product_id}' AND review_date = '{review_date}';
            """
        self.execute(query)

    def insert_review_weekly(self, table, product_id, product_title, review_date):
        review_datetime = datetime.strptime(review_date, "%Y-%m-%d")
        from_date = datetime.strftime(review_datetime - timedelta(days=review_datetime.weekday()), "%Y-%m-%d")
        to_date = datetime.strftime(next_weekday(review_datetime, 0), "%Y-%m-%d")

        query = f"""
        SELECT review_count 
        FROM {table}
        WHERE product_id = '{product_id}' AND from_date = '{from_date}' AND to_date = '{to_date}';
        """
        current_review_count = self.execute(query)

        review_count = 1
        if not current_review_count:
            query = f"""
            INSERT INTO {table} (product_id, product_title, review_count, from_date, to_date)
            VALUES ('{product_id}', '{product_title}', {review_count}, '{from_date}', '{to_date}');
            """
        else:
            review_count = list(current_review_count)[0][0]
            query = f"""
            UPDATE {table}
            SET review_count = {review_count + 1}
            WHERE product_id = '{product_id}' AND from_date = '{from_date}' AND to_date = '{to_date}';
            """
        self.execute(query)

    def insert_review_monthly(self, table, product_id, product_title, review_date):
        review_datetime = datetime.strptime(review_date, "%Y-%m-%d")
        from_date = datetime.strftime(review_datetime - relativedelta(day=1), "%Y-%m-%d")
        to_date = datetime.strftime(review_datetime + relativedelta(day=31), "%Y-%m-%d")

        query = f"""
        SELECT review_count 
        FROM {table}
        WHERE product_id = '{product_id}' AND from_date = '{from_date}' AND to_date = '{to_date}';
        """
        current_review_count = self.execute(query)

        review_count = 1
        if not current_review_count:
            query = f"""
            INSERT INTO {table} (product_id, product_title, review_count, from_date, to_date)
            VALUES ('{product_id}', '{product_title}', {review_count}, '{from_date}', '{to_date}');
            """
        else:
            review_count = list(current_review_count)[0][0]
            query = f"""
            UPDATE {table}
            SET review_count = {review_count + 1}
            WHERE product_id = '{product_id}' AND from_date = '{from_date}' AND to_date = '{to_date}';
            """
        self.execute(query)

    def select_product_reviews(self, table, product_id):
        query = f"""SELECT review_id, product_id, review_headline, review_body FROM {table} 
        WHERE product_id = '{product_id}';
        """
        print(query)
        reviews = list(self.execute(query))

        reviews_json = []
        for review in reviews:
            review_object = {
                "review_id": review[0],
                "product_id": review[1],
                "review_headline": review[2],
                "review_body": review[3]
            }
            reviews_json.append(review_object)
        return json.dumps(reviews_json)

    def select_rated_product_reviews(self, table, product_id, star_rating):
        query = f"""
        SELECT review_id, product_id, star_rating, review_headline, review_body FROM {table} 
        WHERE product_id = '{product_id}' AND star_rating = {star_rating};
        """
        reviews = list(self.execute(query))

        reviews_json = []
        for review in reviews:
            review_object = {
                "review_id": review[0],
                "product_id": review[1],
                "star_rating": review[2],
                "review_headline": review[3],
                "review_body": review[4]
            }
            reviews_json.append(review_object)
        return json.dumps(reviews_json)

    def select_customer_reviews(self, table, customer_id):
        query = f"""
        SELECT customer_id, review_id, product_id, review_headline, review_body  FROM {table} 
        WHERE customer_id = '{customer_id}';
        """
        reviews = list(self.execute(query))

        reviews_json = []
        for review in reviews:
            review_object = {
                "customer_id": review[0],
                "review_id": review[1],
                "product_id": review[2],
                "review_headline": review[3],
                "review_body": review[4]
            }
            reviews_json.append(review_object)
        return json.dumps(reviews_json)

    def get_product_review_count(self, product_id, from_date, to_date):
        months, weeks, days = split_date(from_date, to_date)

        product_review_count = 0
        for month in months:
            query = f"""
            SELECT SUM(review_count) FROM product_review_count_monthly 
            WHERE product_id = '{product_id}' AND from_date >= '{month[1][0]}' AND from_date <= '{month[1][1]}';
            """
            rows = list(self.execute(query))
            product_review_count += rows[0][0] if rows else 0

        for week in weeks:
            query = f"""
            SELECT SUM(review_count) FROM product_review_count_weekly 
            WHERE product_id = '{product_id}' AND from_date >= '{week[1][0]}' AND from_date <= '{week[1][1]}';
            """
            rows = list(self.execute(query))
            product_review_count += rows[0][0] if rows else 0

        for day in days:
            query = f"""
            SELECT SUM(review_count) FROM product_review_count_daily 
            WHERE product_id = '{product_id}' AND review_date >= '{day[1][0]}' AND review_date <= '{day[1][1]}';
            """
            rows = list(self.execute(query))
            product_review_count += rows[0][0] if rows else 0
        return product_review_count

    def select_n_most_reviewed_products(self, n, from_date, to_date):
        unique_products = list(self.execute("SELECT product_id, product_title FROM unique_products;"))

        product_counts = []
        for product in unique_products:
            product_id, product_title = product[0], product[1]
            review_count = self.get_product_review_count(product_id, from_date, to_date)
            product_counts.append((product_id, product_title, review_count))

        # Get N most reviewed items.
        sorted_product_review_counts = sorted(product_counts, key=lambda x: x[2], reverse=True)[:n]

        # Convert to JSON format.
        n_most_reviewed_products = []
        for product in sorted_product_review_counts:
            product_object = {
                "product_id": product[0],
                "product_title": product[1],
                "review_count": product[2]
            }
            n_most_reviewed_products.append(product_object)
        return json.dumps(n_most_reviewed_products)
