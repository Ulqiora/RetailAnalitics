SELECT *
FROM fnc_determination_of_the_group(3, 70, 30);

DROP FUNCTION IF EXISTS fnc_frequency_of_visits(TIMESTAMP, TIMESTAMP, INTEGER, NUMERIC, NUMERIC, NUMERIC);

CREATE FUNCTION fnc_frequency_of_visits(in_first_day TIMESTAMP DEFAULT '2022-08-18 00:00:00',
                                        in_last_day TIMESTAMP DEFAULT '2022-08-18 00:00:00',
                                        in_num_of_transactions INTEGER DEFAULT 1,
                                        in_max_churn_index NUMERIC DEFAULT 3,
                                        in_max_share_of_transactions NUMERIC DEFAULT 70,
                                        in_margin_share NUMERIC DEFAULT 30)

    RETURNS TABLE
        (
        Customer_ID BIGINT,
        Start_Date TIMESTAMP,
        End_Date TIMESTAMP,
        Required_Transactions_Count NUMERIC,
        Group_Name VARCHAR,
        Offer_Discount_Depth NUMERIC
        )
    LANGUAGE plpgsql
AS
$$
BEGIN
RETURN QUERY
SELECT customers_view.customer_id,
       in_first_day,
       in_last_day,
       ROUND((EXTRACT(EPOCH FROM (in_last_day - in_first_day))) /
             EXTRACT(EPOCH FROM (customers_view.customer_frequency))) + in_num_of_transactions,
       dg.Group_name,
       dg.discount_depth
FROM customers_view
         JOIN fnc_determination_of_the_group(in_max_churn_index, in_max_share_of_transactions,
                                             in_margin_share) dg ON customers_view.customer_id = dg.customer_id
ORDER BY Customer_ID;
END;
$$;

-- TEST
SELECT *
FROM fnc_frequency_of_visits();

SELECT *
FROM fnc_frequency_of_visits('2022-01-01 00:00:00',
                             '2022-12-31 00:00:00',
                             1,
                             10,
                             100,
                             50);

SELECT *
FROM fnc_frequency_of_visits('2022-01-01 00:00:00',
                             '2022-12-31 00:00:00',
                             1,
                             100,
                             100,
                             100);
