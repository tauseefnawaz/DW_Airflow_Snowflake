----------------------------------------- Part3.a ---------------

WITH listing_census AS (
    SELECT L.LISTING_ID,L.LISTING_NEIGHBOURHOOD,L.PROPERTY_TYPE,L.ROOM_TYPE,L.ACCOMMODATES,L.PRICE,L.HAS_AVAILABILITY,L.AVAILABILITY_30,L.NUMBER_OF_REVIEWS,L.REVIEW_SCORES_RATING,L.REVIEW_SCORES_CLEANLINESS,L.REVIEW_SCORES_CHECKIN,CEN.AGE_0_4_YR_M,AGE_0_4_YR_F,AGE_0_4_YR_P,CEN.AGE_5_14_YR_M,CEN.AGE_5_14_YR_F,CEN.AGE_5_14_YR_P,CEN.AGE_15_19_YR_M,CEN.AGE_15_19_YR_F,CEN.AGE_15_19_YR_P,CEN.AGE_20_24_YR_M,CEN.AGE_20_24_YR_F,CEN.AGE_20_24_YR_P
    FROM STAGING.STG_LISTING_D L
    INNER JOIN STAGING.STG_LGA_CODE_D C ON L.LISTING_NEIGHBOURHOOD=C.LGA_NAME
    INNER JOIN STAGING.STG_CENSUS_D CEN  ON CEN.LGA_CODE_2016=C.LGA_CODE
    INNER JOIN STAGING.STG_SUBURB_D S ON lower(C.LGA_NAME)=lower(S.LGA_NAME)
    WHERE L.HAS_AVAILABILITY='t'
), 
active_listing AS (
    SELECT LISTING_NEIGHBOURHOOD,avg(review_scores_rating) as avg_review_scores_rating, 
    avg(review_scores_cleanliness) as review_scores_cleanliness,    
    SUM(AGE_0_4_YR_M) AS AGE_0_4_YR_M,
    SUM(AGE_0_4_YR_F) AS AGE_0_4_YR_F,
    SUM(AGE_0_4_YR_P) AS AGE_0_4_YR_P,
    SUM(AGE_5_14_YR_M) AS AGE_5_14_YR_M,
    SUM(AGE_5_14_YR_F) AS AGE_5_14_YR_F,
    SUM(AGE_5_14_YR_P) AS AGE_5_14_YR_P,
    SUM(AGE_15_19_YR_M) AS AGE_15_19_YR_M,
    SUM(AGE_15_19_YR_F) AS AGE_15_19_YR_F,
    SUM(AGE_15_19_YR_P) AS AGE_15_19_YR_P,
    SUM(AGE_20_24_YR_M) AS AGE_20_24_YR_M,
    SUM(AGE_20_24_YR_F) AS AGE_20_24_YR_F,
    SUM(AGE_20_24_YR_P) AS AGE_20_24_YR_P,
    sum(30-AVAILABILITY_30) as total_staying,
    sum(price) as price
    FROM listing_census
    GROUP BY LISTING_NEIGHBOURHOOD
) SELECT *,total_staying*price as revenue FROM active_listing order by revenue desc;
----------------------------------------- Part3.b ---------------
WITH listing_census AS (
    SELECT L.LISTING_ID,L.LISTING_NEIGHBOURHOOD,L.PROPERTY_TYPE,L.ROOM_TYPE,L.ACCOMMODATES,L.PRICE,L.HAS_AVAILABILITY,L.AVAILABILITY_30
    FROM STAGING.STG_LISTING_D L
    WHERE L.HAS_AVAILABILITY='t'
), 
active_listing AS (
    SELECT LISTING_NEIGHBOURHOOD,PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,
    sum(30-AVAILABILITY_30) as total_staying,
    sum(price) as price
    FROM listing_census
    GROUP BY LISTING_NEIGHBOURHOOD,PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES
) SELECT *,total_staying*price as revenue FROM active_listing order by total_staying desc,revenue desc;
------------------------------------------- Part3.c  --------
WITH same_neighbourhood_cte AS (
        SELECT host_name,count(*) as same_neighbourhood FROM LISTING_01_2021 
        where host_neighbourhood=listing_neighbourhood
        group by host_name
), different_neighbourhood_cte AS (
        SELECT host_name,count(*) as different_neighbourhood FROM LISTING_01_2021 
        where host_neighbourhood!=listing_neighbourhood
        group by host_name
) SELECT s.host_name,s.same_neighbourhood,d.different_neighbourhood from same_neighbourhood_cte s INNER JOIN different_neighbourhood_cte d ON s.host_name=d.host_name order by CTE2.total_listing desc;
------------------------------------------- Part3.d  --------
WITH listing_census AS (
    SELECT L.LISTING_NEIGHBOURHOOD,L.PRICE,L.AVAILABILITY_30, CEN.MEDIAN_MORTGAGE_REPAY_MONTHLY
    FROM STAGING.STG_LISTING_D L
    INNER JOIN STAGING.STG_LGA_CODE_D C ON L.LISTING_NEIGHBOURHOOD=C.LGA_NAME
    INNER JOIN STAGING.STG_CENSUS_D CEN  ON CEN.LGA_CODE_2016=C.LGA_CODE
    INNER JOIN STAGING.STG_SUBURB_D S ON lower(C.LGA_NAME)=lower(S.LGA_NAME)
), 
get_result_cte AS (
    SELECT LISTING_NEIGHBOURHOOD,sum(30-AVAILABILITY_30) as total_staying,sum(price) as price,
    sum(MEDIAN_MORTGAGE_REPAY_MONTHLY) as MEDIAN_MORTGAGE_ANNAULLY
    FROM listing_census
    GROUP BY LISTING_NEIGHBOURHOOD
)SELECT LISTING_NEIGHBOURHOOD,MEDIAN_MORTGAGE_ANNAULLY,price*total_staying as estimated_revenue FROM get_result_cte order by estimated_revenue desc,MEDIAN_MORTGAGE_ANNAULLY desc;