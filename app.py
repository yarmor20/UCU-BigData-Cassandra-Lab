from fastapi import FastAPI
from src.cassandra_client import CassandraClient


app = FastAPI()
client = CassandraClient(host="cassandra-node", port=9042, keyspace="lab4_keyspace")
client.connect()


@app.get("/")
def root():
    return {"Hello": "World"}


@app.get("/query1/{product_id}")
async def get_product_reviews(product_id: str):
    reviews = client.select_product_reviews("product_reviews", product_id)
    return reviews


@app.get("/query2/{product_id}/{star_rating}")
async def get_product_reviews(product_id: str, star_rating: int):
    reviews = client.select_rated_product_reviews("product_rated_reviews", product_id, star_rating)
    return reviews


@app.get("/query3/{customer_id}")
async def get_product_reviews(customer_id: str):
    reviews = client.select_customer_reviews("customer_reviews", customer_id)
    return reviews


@app.get("/query4/{n}/{from_date}/{to_date}")
async def get_product_reviews(n: int, from_date: str, to_date: str):
    reviews = client.select_n_most_reviewed_products(n, from_date, to_date)
    return reviews


# if __name__ == '__main__':
#     import uvicorn
#     uvicorn.run(app, port=8080)
#
#     client.close()
