-- 1
CREATE TABLE product_reviews (
    review_id text,
    product_id text,
    review_headline text,
    review_body text,
    review_date date,
    PRIMARY KEY((product_id), review_id, review_date)
) WITH CLUSTERING ORDER BY (review_id ASC, review_date DESC);

-- 2
CREATE TABLE product_rated_reviews (
    review_id text,
    product_id text,
    star_rating int,
    review_headline text,
    review_body text,
    review_date date,
    PRIMARY KEY((product_id, star_rating), review_id, review_date)
) WITH CLUSTERING ORDER BY (review_id ASC, review_date DESC);

-- 3
CREATE TABLE customer_reviews (
    customer_id text,
    review_id text,
    product_id text,
    review_headline text,
    review_body text,
    review_date date,
    PRIMARY KEY((customer_id), product_id, review_date)
) WITH CLUSTERING ORDER BY (product_id ASC, review_date DESC);

-- 4
CREATE TABLE unique_products (
    product_id text,
    product_title text,
    PRIMARY KEY ((product_id))
);

CREATE TABLE product_review_count_daily (
    product_id text,
    product_title text,
    review_count int,
    review_date date,
    PRIMARY KEY ((product_id), review_date)
) WITH CLUSTERING ORDER BY (review_date DESC);

CREATE TABLE product_review_count_weekly (
    product_id text,
    product_title text,
    review_count int,
    from_date date,
    to_date date,
    PRIMARY KEY ((product_id), from_date, to_date)
) WITH CLUSTERING ORDER BY (from_date DESC);

CREATE TABLE product_review_count_monthly (
    product_id text,
    product_title text,
    review_count int,
    from_date date,
    to_date date,
    PRIMARY KEY ((product_id), from_date, to_date)
) WITH CLUSTERING ORDER BY (from_date DESC);
