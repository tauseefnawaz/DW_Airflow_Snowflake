CREATE OR REPLACE TABLE DATAMART.dm_listing_neighbourhood AS WITH dm_listing_neighbourhood_cte as (
    WITH listings AS (
        SELECT L.LISTING_ID,L.LISTING_NEIGHBOURHOOD,L.PRICE,L.HAS_AVAILABILITY,L.AVAILABILITY_30,L.REVIEW_SCORES_RATING,H.HOST_NAME,H.HOST_IS_SUPERHOST, LEFT(H.SCRAPED_DATE,7) as S_DATE
        FROM DATAWAREHOUSE.LISTING_d as L
        INNER JOIN DATAWAREHOUSE.HOSTING_d as H ON L.LISTING_ID=H.LISTING_ID
    ),
    Active_listings_cte AS (
        SELECT LISTING_NEIGHBOURHOOD,S_DATE,count(LISTING_ID) as active_listing_count,min(price) as min_price,max(price) as max_price,avg(price) as avg_price,median(price) as median_price,count(distinct HOST_NAME) as distinct_host_count,avg(review_scores_rating) as avg_review_scores_rating ,sum(30-AVAILABILITY_30) as total_staying,sum(price) as price
        FROM listings
        WHERE HAS_AVAILABILITY='t'
        GROUP BY LISTING_NEIGHBOURHOOD,S_DATE
    ),
    All_listings_count_cte AS (
        SELECT LISTING_NEIGHBOURHOOD,S_DATE,count(LISTING_ID) as all_listing_count,count(host_is_superhost)as all_count_host_is_superhost
        FROM listings
        GROUP BY LISTING_NEIGHBOURHOOD,S_DATE
    ),
    host_is_superhost_cte AS (
        SELECT LISTING_NEIGHBOURHOOD,S_DATE,COUNT(host_is_superhost) as count_host_is_superhost
        FROM listings
        WHERE host_is_superhost='t'
        GROUP BY LISTING_NEIGHBOURHOOD,S_DATE
    ), count_for_percentage_change AS (
        SELECT count(LISTING_ID) as active_listing_count,S_DATE
        from listings
        GROUP BY S_DATE
    ), count_for_percentage_change_active AS (
        SELECT count(LISTING_ID) as active_listing_count,S_DATE
        from listings
        where HAS_AVAILABILITY='t'
        GROUP BY S_DATE
    ), count_for_percentage_change_inactive AS (
        SELECT count(LISTING_ID) as inactive_listing_count,S_DATE
        from listings
        where HAS_AVAILABILITY='f'
        GROUP BY S_DATE
    ),percentage_change_cte AS (
        SELECT active.S_DATE, active.active_listing_count AS active_final_value, 
        Lag(active_listing_count, 1) OVER(ORDER BY active.S_DATE ASC) AS active_original_value,
        (active_final_value-active_original_value)/active_original_value*100 as active_percentage_change,
        inactive.inactive_listing_count AS inactive_final_value, 
        Lag(inactive_final_value, 1) OVER(ORDER BY inactive.S_DATE ASC) AS inactive_original_value,
        (inactive_final_value-inactive_original_value)/inactive_original_value*100 as inactive_percentage_change
        from count_for_percentage_change_active active
        INNER JOIN count_for_percentage_change_inactive inactive ON active.S_DATE=inactive.S_DATE
    )
    SELECT (alc.active_listing_count/alcc.all_listing_count)*100 as active_listing_rate,alc.min_price,alc.max_price,alc.avg_price,alc.median_price,alc.distinct_host_count,avg_review_scores_rating,pc.active_percentage_change,pc.inactive_percentage_change,sh.count_host_is_superhost/alcc.all_count_host_is_superhost*100 as Superhost_rate,alc.total_staying,alc.total_staying*alc.price as average_estimated_revenue
    FROM Active_listings_cte as alc 
    INNER JOIN All_listings_count_cte as alcc ON alc.LISTING_NEIGHBOURHOOD=alcc.LISTING_NEIGHBOURHOOD AND alc.S_DATE=alcc.S_DATE
    INNER JOIN host_is_superhost_cte  as sh   ON alc.LISTING_NEIGHBOURHOOD=sh.LISTING_NEIGHBOURHOOD      AND alc.S_DATE=sh.S_DATE
    INNER JOIN percentage_change_cte as pc ON alc.S_DATE=pc.S_DATE
    ORDER BY alc.LISTING_NEIGHBOURHOOD,alc.S_DATE
) SELECT * FROM dm_listing_neighbourhood_cte;

CREATE OR REPLACE TABLE DATAMART.dm_property_type AS WITH dm_property_type_cte as (
    WITH listings AS (
        SELECT L.LISTING_ID,L.PROPERTY_TYPE,L.ROOM_TYPE,L.ACCOMMODATES,L.PRICE,L.HAS_AVAILABILITY,L.AVAILABILITY_30,L.REVIEW_SCORES_RATING,H.HOST_NAME,H.HOST_IS_SUPERHOST, LEFT(H.SCRAPED_DATE,7) as S_DATE
        FROM DATAWAREHOUSE.LISTING_d as L
        INNER JOIN DATAWAREHOUSE.HOSTING_d as H ON L.LISTING_ID=H.LISTING_ID
    ),
    Active_listings_cte AS (
        SELECT PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,S_DATE,count(LISTING_ID) as active_listing_count,min(price) as min_price,max(price) as max_price,avg(price) as avg_price,median(price) as median_price,count(distinct HOST_NAME) as distinct_host_count,avg(review_scores_rating) as avg_review_scores_rating ,sum(30-AVAILABILITY_30) as total_staying,sum(price) as price
        FROM listings
        WHERE HAS_AVAILABILITY='t'
        GROUP BY PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,S_DATE
    ),
    All_listings_count_cte AS (
        SELECT PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,S_DATE,count(LISTING_ID) as all_listing_count,count(host_is_superhost)as all_count_host_is_superhost
        FROM listings
        GROUP BY PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,S_DATE
    ),
    host_is_superhost_cte AS (
        SELECT PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,S_DATE,COUNT(host_is_superhost) as count_host_is_superhost
        FROM listings
        WHERE host_is_superhost='t'
        GROUP BY PROPERTY_TYPE,ROOM_TYPE,ACCOMMODATES,S_DATE
    ), 
    count_for_percentage_change_active AS (
        SELECT count(LISTING_ID) as active_listing_count,S_DATE
        from listings
        where HAS_AVAILABILITY='t'
        GROUP BY S_DATE
    ), 
    count_for_percentage_change_inactive AS (
        SELECT count(LISTING_ID) as inactive_listing_count,S_DATE
        from listings
        where HAS_AVAILABILITY='f'
        GROUP BY S_DATE
    ),
    percentage_change_cte AS (
        SELECT active.S_DATE, active.active_listing_count AS active_final_value, 
        Lag(active_listing_count, 1) OVER(ORDER BY active.S_DATE ASC) AS active_original_value,
        (active_final_value-active_original_value)/active_original_value*100 as active_percentage_change,
        inactive.inactive_listing_count AS inactive_final_value, 
        Lag(inactive_final_value, 1) OVER(ORDER BY inactive.S_DATE ASC) AS inactive_original_value,
        (inactive_final_value-inactive_original_value)/inactive_original_value*100 as inactive_percentage_change
        from count_for_percentage_change_active active
        INNER JOIN count_for_percentage_change_inactive inactive ON active.S_DATE=inactive.S_DATE
    )
    SELECT (alc.active_listing_count/alcc.all_listing_count)*100 as active_listing_rate,alc.min_price,alc.max_price,alc.avg_price,alc.median_price,alc.distinct_host_count,avg_review_scores_rating,pc.active_percentage_change,pc.inactive_percentage_change,sh.count_host_is_superhost/alcc.all_count_host_is_superhost*100 as Superhost_rate,alc.total_staying,alc.total_staying*alc.price as average_estimated_revenue
    FROM Active_listings_cte as alc 
    INNER JOIN All_listings_count_cte as alcc ON alc.PROPERTY_TYPE=alcc.PROPERTY_TYPE AND alc.ROOM_TYPE=alcc.ROOM_TYPE AND alc.ACCOMMODATES=alcc.ACCOMMODATES AND alc.S_DATE=alcc.S_DATE
    INNER JOIN host_is_superhost_cte  as sh   ON alc.PROPERTY_TYPE=sh.PROPERTY_TYPE   AND alc.ROOM_TYPE=sh.ROOM_TYPE   AND alc.ACCOMMODATES=sh.ACCOMMODATES   AND alc.S_DATE=sh.S_DATE
    INNER JOIN percentage_change_cte as pc ON alc.S_DATE=pc.S_DATE
    ORDER BY alc.PROPERTY_TYPE,alc.ROOM_TYPE,alc.ACCOMMODATES,alc.S_DATE
) SELECT * FROM dm_property_type_cte;

CREATE OR REPLACE TABLE DATAMART.dm_host_neighbourhood AS WITH dm_host_neighbourhood_cte AS (
    WITH listings AS (
        SELECT L.LISTING_ID,L.LISTING_NEIGHBOURHOOD,L.PRICE,L.AVAILABILITY_30,H.HOST_NAME, H.HOST_NEIGHBOURHOOD,LEFT(H.SCRAPED_DATE,7) as S_DATE
        FROM DATAWAREHOUSE.LISTING_d as L
        INNER JOIN DATAWAREHOUSE.HOSTING_d as H ON L.LISTING_ID=H.LISTING_ID
    ),
    COUNT_HOST_CTE AS (
        SELECT HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD,count(*) as counting FROM listings 
        GROUP BY HOST_NEIGHBOURHOOD,LISTING_NEIGHBOURHOOD
    ),
    MAX_HOST_CTE AS (
        SELECT HOST_NEIGHBOURHOOD,max(counting) as counting FROM COUNT_HOST_CTE
        GROUP BY HOST_NEIGHBOURHOOD
    ),
    CREATE_LGA_HOST_NEIGHBOURHOOD AS (
        SELECT c1.HOST_NEIGHBOURHOOD,c1.LISTING_NEIGHBOURHOOD as host_neighbourhood_lga FROM COUNT_HOST_CTE c1 INNER JOIN MAX_HOST_CTE c2 ON c1.HOST_NEIGHBOURHOOD=c2.HOST_NEIGHBOURHOOD AND c1.counting=c2.counting 
    ),
    dm_property_type_cte AS (
        SELECT host_neighbourhood_lga,S_DATE,count(distinct HOST_NAME) as distinct_host_count,sum(30-AVAILABILITY_30) as total_staying,sum(price) as price
        FROM listings l
        INNER JOIN CREATE_LGA_HOST_NEIGHBOURHOOD lga ON l.HOST_NEIGHBOURHOOD=lga.HOST_NEIGHBOURHOOD AND l.LISTING_NEIGHBOURHOOD=lga.host_neighbourhood_lga
        GROUP BY host_neighbourhood_lga,S_DATE
    )
    SELECT host_neighbourhood_lga,S_DATE,distinct_host_count,total_staying*price as average_estimated_revenue,average_estimated_revenue/distinct_host_count as average_revenue_p_host
    FROM dm_property_type_cte
    ORDER BY host_neighbourhood_lga,S_DATE
) SELECT * FROM dm_host_neighbourhood_cte;