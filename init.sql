CREATE TABLE IF NOT EXISTS dw_article_data (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    date DATE NOT NULL,
    link TEXT NOT NULL,
    thumbnail TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (link)
);

CREATE TABLE public.dw_marketing_data (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "uid" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    primary_categories TEXT NOT NULL,
    categories TEXT NOT NULL,
    "condition" TEXT NOT NULL,
    "availability" TEXT NOT NULL,
    brand TEXT NOT NULL,
    merchant TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_max NUMERIC NOT NULL,
    amount_min NUMERIC NOT NULL,
    shipping NUMERIC NOT NULL,
    is_sale BOOLEAN NOT NULL,
    size TEXT NOT NULL,
    unit TEXT NOT NULL,
    manufacturer TEXT NOT NULL,
    manufacturer_number TEXT NOT NULL,
    source_urls TEXT NOT NULL,
    image_urls TEXT NOT NULL,
    asins TEXT NOT NULL,
    ean TEXT NOT NULL,
    upc TEXT NOT NULL,
    keys TEXT NOT NULL,
    date_seen TEXT NOT NULL NOT NULL,
    date_added DATE NOT NULL NOT NULL,
    date_updated DATE NOT NULL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE public.dw_sales_data (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "name" TEXT NOT NULL,
    main_category TEXT NOT NULL,
    sub_category TEXT NOT NULL,
    image TEXT NOT NULL,
    link TEXT NOT NULL,
    ratings NUMERIC NOT NULL,
    no_of_ratings INTEGER NOT NULL,
    discount_price NUMERIC NOT NULL,
    actual_price NUMERIC NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("name", main_category, sub_category)
);
