listing_insert = """
        INSERT INTO STAGING.STG_LISTING_d(LISTING_ID,
            LISTING_NEIGHBOURHOOD,
            PROPERTY_TYPE,
            ROOM_TYPE,
            ACCOMMODATES,
            PRICE,
            HAS_AVAILABILITY,
            AVAILABILITY_30,
            NUMBER_OF_REVIEWS,
            REVIEW_SCORES_RATING,
            REVIEW_SCORES_ACCURACY,
            REVIEW_SCORES_CLEANLINESS,
            REVIEW_SCORES_CHECKIN,
            REVIEW_SCORES_COMMUNICATION,
            REVIEW_SCORES_VALUE) 
             SELECT LISTING_ID,
                LISTING_NEIGHBOURHOOD,
                PROPERTY_TYPE,
                ROOM_TYPE,
                ACCOMMODATES,
                PRICE,
                HAS_AVAILABILITY,
                AVAILABILITY_30,
                NUMBER_OF_REVIEWS,
                REVIEW_SCORES_RATING,
                REVIEW_SCORES_ACCURACY,
                REVIEW_SCORES_CLEANLINESS,
                REVIEW_SCORES_CHECKIN,
                REVIEW_SCORES_COMMUNICATION,
                REVIEW_SCORES_VALUE 
            FROM STAGING."""
host_insert = """INSERT INTO STAGING.STG_HOSTING_d(
                    LISTING_ID,
                    SCRAPE_ID,
                    SCRAPED_DATE,
                    HOST_ID,
                    HOST_NAME,
                    HOST_SINCE,
                    HOST_IS_SUPERHOST,
                    HOST_NEIGHBOURHOOD 
                ) SELECT LISTING_ID,
                    SCRAPE_ID,
                    SCRAPED_DATE,
                    HOST_ID,
                    HOST_NAME,
                    HOST_SINCE,
                    HOST_IS_SUPERHOST,
                    HOST_NEIGHBOURHOOD  FROM STAGING."""
    
fact_table_query = """CREATE OR REPLACE TABLE STAGING.STG_AT3_f AS WITH AT3_fact_table_cte AS (
    SELECT L.ID as LISTING_ID,H.ID as HOST_ID,CEN.ID as CENSUS_ID,C.ID AS LGA_CODE_ID, S.ID as SURURB_ID,H.SCRAPED_DATE as DATE,L.PRICE 
    FROM STAGING.STG_LISTING_D L
    INNER JOIN STAGING.STG_HOSTING_D H ON L.LISTING_ID=H.LISTING_ID
    INNER JOIN STAGING.STG_LGA_CODE_D C ON L.LISTING_NEIGHBOURHOOD=C.LGA_NAME
    INNER JOIN STAGING.STG_CENSUS_D CEN  ON CEN.LGA_CODE_2016=C.LGA_CODE
    INNER JOIN STAGING.STG_SUBURB_D S ON lower(C.LGA_NAME)=lower(S.LGA_NAME)
) SELECT * FROM AT3_fact_table_cte;"""
