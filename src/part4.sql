DROP FUNCTION IF EXISTS fnc_personal_offers_aimed_at_the_growth_of_the_average_check cascade;
DROP FUNCTION IF EXISTS fnc_by_period cascade;
DROP FUNCTION IF EXISTS fnc_by_the_number_of_recent_transactions cascade;


CREATE OR REPLACE FUNCTION fnc_personal_offers_aimed_at_the_growth_of_the_average_check(calculation_method INT,
                                                                                        first_date CHAR,
                                                                                        last_date CHAR,
                                                                                        tr_count INT,
                                                                                        coefficient_of_average_check_increase NUMERIC,
                                                                                        max_churn_index NUMERIC,
                                                                                        max_share_of_tr_with_a_discount NUMERIC,
                                                                                        allowable_share_of_margin NUMERIC)
    RETURNS TABLE
            (
                Customer_ID            BIGINT,
                Required_Check_Measure NUMERIC,
                Group_Name             VARCHAR,
                Offer_Discount_Depth   NUMERIC
            )
AS
$$
DECLARE
    check_first_date DATE := (SELECT MAX(transaction_datetime)
                              FROM transactions);
    check_last_date  DATE := (SELECT MIN(transaction_datetime)
                              FROM transactions);
    first_date_d     DATE := to_date(first_date, 'YYYY/MM/DD');
    last_date_d      DATE := to_date(last_date, 'YYYY/MM/DD');
BEGIN
    IF (calculation_method = 1) THEN
        IF first_date_d > last_date_d THEN
            RAISE EXCEPTION 'First date (%) must be less than last date (%)', first_date_d, last_date_d;
        ELSEIF first_date_d > check_first_date THEN
            first_date_d := check_first_date;
        ELSEIF
            last_date_d < check_last_date THEN
            last_date_d := check_last_date;
        END IF;
        RETURN QUERY (SELECT pi.customer_id,
                             fbp.Average_check * coefficient_of_average_check_increase,
                             fdotg.Group_name,
                             fdotg.Discount_depth
                      FROM personal_information pi
                               JOIN
                           fnc_by_period(first_date, last_date) fbp ON pi.customer_id = fbp.Customer_ID
                               JOIN fnc_determination_of_the_group(max_churn_index, max_share_of_tr_with_a_discount,
                                                                   allowable_share_of_margin) fdotg
                                    ON pi.customer_id = fdotg.Customer_ID);
    ELSEIF
        (calculation_method = 2) THEN
        RETURN QUERY (SELECT pi.customer_id,
                             fbnrt.Average_check * coefficient_of_average_check_increase,
                             fdotg.Group_name,
                             fdotg.Discount_depth
                      FROM personal_information pi
                               JOIN
                           fnc_by_the_number_of_recent_transactions(tr_count) fbnrt
                           ON pi.customer_id = fbnrt.Customer_ID
                               JOIN fnc_determination_of_the_group(max_churn_index, max_share_of_tr_with_a_discount,
                                                                   allowable_share_of_margin) fdotg
                                    ON pi.customer_id = fdotg.Customer_ID);
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fnc_by_period(first_date CHAR DEFAULT NULL, last_date CHAR DEFAULT NULL)
    RETURNS TABLE
            (
                Customer_ID   BIGINT,
                Average_check NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT pi.customer_id, (SUM(t.transaction_summ) / COUNT(t.transaction_id))
                  FROM transactions t
                           JOIN cards c ON c.customer_card_id = t.customer_card_id
                           JOIN personal_information pi ON pi.customer_id = c.customer_id
                  WHERE t.transaction_datetime BETWEEN to_date(first_date, 'YYYY/MM/DD') AND to_date(last_date, 'YYYY/MM/DD')
                  GROUP BY pi.customer_id);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_by_the_number_of_recent_transactions(tr_count INT)
    RETURNS TABLE
            (
                Customer_ID   BIGINT,
                Average_check NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY (with last_tr AS (SELECT pi.customer_id, (sum(tmp.transaction_summ) / count(tmp.rn)) AS sum, rn
                                   FROM personal_information pi
                                            JOIN (SELECT p.customer_id,
                                                         row_number()
                                                         OVER (PARTITION BY p.customer_id ORDER BY t2.transaction_datetime DESC) AS rn,
                                                         t2.transaction_summ
                                                  FROM personal_information p
                                                           JOIN cards c2 ON p.customer_id = c2.customer_id
                                                           JOIN transactions t2 ON c2.customer_card_id = t2.customer_card_id
                                                  GROUP BY p.customer_id, t2.transaction_datetime, t2.transaction_summ) tmp
                                                 ON tmp.customer_id = pi.customer_id
                                   WHERE tmp.rn <= tr_count
                                   GROUP BY pi.customer_id, rn)

                  SELECT last_tr.customer_id, sum(sum) / count(rn)
                  FROM last_tr
                  GROUP BY last_tr.customer_id);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fnc_determination_of_the_group(in_max_churn_index NUMERIC,
                                                          in_max_share_of_transactions NUMERIC,
                                                          in_margin_share NUMERIC)

    RETURNS TABLE
            (
                Customer_ID    BIGINT,
                Group_name     VARCHAR,
                Discount_depth NUMERIC
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH tmp AS (SELECT DISTINCT general.customer_id,
                                     general.group_id,
                                     general.group_name,
                                     groups.group_affinity_index,
                                     (CEIL((group_minimum_discount * 100) / 5) * 5) AS discount
                     FROM general
                              JOIN groups ON general.customer_id = groups.customer_id AND
                                             general.group_id = groups.group_id
                     WHERE groups.group_minimum_discount > 0
                       AND groups.group_churn_rate < in_max_churn_index
                       AND groups.group_discount_share < (in_max_share_of_transactions::numeric / 100.0)
                       AND CEIL((group_minimum_discount * 100) / 5) * 5 <
                           ((sku_retail_price - general.sku_purchase_price) * in_margin_share /
                            general.sku_retail_price))
        SELECT tmp.customer_id, tmp.group_name, tmp.discount
        FROM tmp
        WHERE (tmp.customer_id, tmp.group_affinity_index) IN
              (SELECT tmp.customer_id, max(tmp.group_affinity_index) FROM tmp GROUP BY tmp.customer_id)
        ORDER BY Customer_ID;
END;
$$;


SELECT *
FROM fnc_personal_offers_aimed_at_the_growth_of_the_average_check(2, '',
                                                                  '',
                                                                  100,
                                                                  1.15,
                                                                  3,
                                                                  70,
                                                                  30);

SELECT *
FROM fnc_personal_offers_aimed_at_the_growth_of_the_average_check(1, '2022-01-01',
                                                                  '2022-12-31',
                                                                  9,
                                                                  1,
                                                                  10,
                                                                  100,
                                                                  50);
