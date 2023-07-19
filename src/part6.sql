DROP FUNCTION IF EXISTS fnc_cross_selling(INTEGER, NUMERIC, NUMERIC, NUMERIC, NUMERIC);

CREATE FUNCTION fnc_cross_selling(in_num_of_groups INTEGER DEFAULT 5,
                                  in_max_churn_index NUMERIC DEFAULT 3,
                                  in_max_stability_index NUMERIC DEFAULT 0.5,
                                  in_max_sku_share NUMERIC DEFAULT 100,
                                  in_allow_margin_share NUMERIC DEFAULT 30)

    RETURNS TABLE
        (
        Customer_ID BIGINT,
        SKU_Name VARCHAR,
        Offer_Discount_Depth NUMERIC
        )
    LANGUAGE plpgsql
AS
$$
BEGIN
RETURN QUERY WITH tmp1 AS (SELECT lol1.customer_id, lol1.group_id, customer_primary_store
                      FROM (SELECT *,
                                   ROW_NUMBER()
                                   OVER (PARTITION BY groups.customer_id ORDER BY groups.group_affinity_index DESC) AS num
                            FROM groups
                            WHERE groups.group_churn_rate <= in_max_churn_index
                              AND groups.group_stability_index < in_max_stability_index) AS lol1
                               JOIN customers_view ON customers_view.customer_id = lol1.customer_id
                      WHERE num <= in_num_of_groups),
             tmp2 AS (SELECT tmp1.customer_id,
                             tmp1.group_id,
                             tmp1.customer_primary_store,
                             lol2.sku_id,
                             lol2.diff_price,
                             lol2.sku_retail_price
                      FROM tmp1
                               JOIN (SELECT stores.transaction_store_id,
                                            stores.sku_id,
                                            stores.sku_retail_price,
                                            stores.sku_retail_price - stores.sku_purchase_price                      AS diff_price,
                                            product_grid.group_id,
                                            ROW_NUMBER()
                                            OVER (PARTITION BY
                                                stores.transaction_store_id,
                                                product_grid.group_id
                                                ORDER BY (stores.sku_retail_price - stores.sku_purchase_price) DESC) AS num
                                     FROM stores
                                              JOIN product_grid on stores.sku_id = product_grid.sku_id) AS lol2
                                    ON tmp1.group_id = lol2.group_id AND
                                       tmp1.customer_primary_store = lol2.transaction_store_id
                      WHERE lol2.num = 1),
             tmp3 AS (SELECT t1.group_id, t1.sku_id, t1.tr::NUMERIC / t2.gr AS SKU_share
                      FROM (SELECT group_id, sku_id, COUNT(DISTINCT transaction_id) AS tr
                            FROM general
                            GROUP BY group_id, sku_id) AS t1
                               JOIN
                           (SELECT group_id, COUNT(DISTINCT transaction_id) AS gr FROM general GROUP BY group_id) AS t2
                           ON t1.group_id = t2.group_id),
             tmp4 AS (SELECT tmp2.customer_id,
                             tmp2.group_id,
                             tmp2.customer_primary_store,
                             tmp2.sku_id,
                             (tmp2.diff_price * in_allow_margin_share::NUMERIC / 100) /
                             tmp2.sku_retail_price                          AS discount,
                             CEIL(periods.group_min_discount / 0.05) * 0.05 AS min_discount
                      FROM tmp2
                               JOIN periods
                                    ON tmp2.customer_id = periods.customer_id AND tmp2.group_id = periods.group_id
                               JOIN tmp3 ON tmp2.sku_id = tmp3.sku_id AND tmp2.group_id = tmp3.group_id
                      WHERE tmp3.SKU_share <= in_max_sku_share::NUMERIC / 100)

SELECT tmp4.customer_id, product_grid.sku_name, tmp4.min_discount * 100
FROM tmp4
         JOIN product_grid ON tmp4.sku_id = product_grid.sku_id
WHERE discount >= min_discount;
END;
$$;

-- TEST
SELECT *
FROM fnc_cross_selling();

SELECT *
FROM fnc_cross_selling(1, 2, 0.9, 100, 90);

SELECT *
FROM fnc_cross_selling(5, 3, 0.9, 80, 100);
