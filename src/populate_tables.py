from cassandra_write import CassandraClient


def populate_tables(cclient):
    with open("../data/amazon_reviews_us_Books_v1_02.tsv", "r") as source_data:
        count = sum(1 for _ in source_data)

    # Read only 20% of file.
    max_count = count // 5

    curr_count = 0
    with open("../data/amazon_reviews_us_Books_v1_02.tsv", "r") as source_data:
        # Skip header.
        source_data.readline()

        while curr_count != max_count:
            myline = source_data.readline()
            if not myline:
                break

            # Retrieve required fields.
            line = myline.strip().split("\t")
            customer_id, review_id, product_id = line[1], line[2], line[3]
            product_title = line[5].replace("'", "`")
            star_rating = line[7]
            review_headline, review_body, review_date = line[12].replace("'", "`"), line[13].replace("'", "`"), line[14]

            # Populate tables.
            cclient.insert_unique_products("unique_products", product_id, product_title)
            cclient.insert_product_reviews("product_reviews", review_id, product_id, review_headline, review_body, review_date)
            cclient.insert_rated_reviews("product_rated_reviews", review_id, product_id, star_rating, review_headline, review_body, review_date)
            cclient.insert_customer_reviews("customer_reviews", customer_id, review_id, product_id, review_headline, review_body, review_date)
            cclient.insert_review_daily("product_review_count_daily", product_id, product_title, review_date)
            cclient.insert_review_weekly("product_review_count_weekly", product_id, product_title, review_date)
            cclient.insert_review_monthly("product_review_count_monthly", product_id, product_title, review_date)
            curr_count += 1


if __name__ == '__main__':
    client = CassandraClient(host="localhost", port=9042, keyspace="lab4_keyspace")
    client.connect()

    populate_tables(client)

    client.close()
