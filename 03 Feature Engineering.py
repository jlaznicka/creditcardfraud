import snowflake.snowpark as snp
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark import Window

# from snowflake.snowpark import Session, Window
def main(session: snp.Session):     

    ################################################################################################################
    # Date and Time Transformations
    ################################################################################################################
    
    df_cust_trx_fraud = session.table("CUSTOMER_TRANSACTIONS_FRAUD")
    df_cust_trx_fraud.show()
    
    df_date_time_feat = df_cust_trx_fraud.with_columns(["TX_DURING_WEEKEND",  "TX_DURING_NIGHT"],\
                                                [F.iff((F.dayofweek(F.col("TX_DATETIME")) == F.lit(6)) \
                                                       | (F.dayofweek(F.col("TX_DATETIME")) == F.lit(0)), F.lit(1), F.lit(0)),
                                                 F.iff((F.hour(F.col("TX_DATETIME")) < F.lit(6)) \
                                                       | (F.hour(F.col("TX_DATETIME")) > F.lit(20)), F.lit(1), F.lit(0))])
    return df_date_time_feat
    '''
    ################################################################################################################
    # Customer Spending Behaviour Transformations
    ################################################################################################################

    date_info = df_cust_trx_fraud.select(F.min(F.col("TX_DATETIME")).as_("END_DATE"), 
                                     F.datediff("DAY", F.col("END_DATE"), F.max(F.col("TX_DATETIME"))).as_("NO_DAYS")).to_pandas()
    days = int(date_info['NO_DAYS'].values[0])
    start_date = str(date_info['END_DATE'].values[0].astype('datetime64[D]'))
    
    # Create a dataframe with one row for each date between the min and max transaction date
    df_days = session.range(days).with_column("TX_DATE", F.to_date(F.dateadd("DAY", F.seq4(), F.lit(start_date))))
    
    # Since we aggregate by customer and day and not all customers have transactions for all dates we cross join our date dataframe with our 
    # customer table so each customer witll have one row for each date
    df_customers = session.table("CUSTOMERS").select("CUSTOMER_ID")
    df_cust_day = df_days.join(df_customers)
    
    # ID of distinct days joined with all our customer IDs
    ######## return df_cust_day

    # Number of transactions and amount by customer and day
    zero_if_null = F.function("ZEROIFNULL")

    df_cust_trx_day = df_cust_trx_fraud.join(df_cust_day, (df_cust_trx_fraud["CUSTOMER_ID"] == df_cust_day["CUSTOMER_ID"]) \
                                             & (F.to_date(df_cust_trx_fraud["TX_DATETIME"]) == df_cust_day["TX_DATE"]), "rightouter") \
                .select(df_cust_day.col("CUSTOMER_ID").as_("CUSTOMER_ID"),\
                        df_cust_day.col("TX_DATE"),\
                        zero_if_null(df_cust_trx_fraud["TX_AMOUNT"]).as_("TX_AMOUNT"),\
                        F.iff(F.col("TX_AMOUNT") > F.lit(0), F.lit(1), F.lit(0)).as_("NO_TRX"))\
                    .group_by(F.col("CUSTOMER_ID"), F.col("TX_DATE"))\
                    .agg([F.sum(F.col("TX_AMOUNT")).as_("TOT_AMOUNT"), F.sum(F.col("NO_TRX")).as_("NO_TRX")])
    
    ######## return df_cust_trx_day

    # Now when we have the number of transactions and amount by customer and day we can aggregate by our windows (1, 7 and 30 days).
    cust_date = Window.partition_by(F.col("customer_id")).orderBy(F.col("TX_DATE"))
    win_7d_cust = cust_date.rowsBetween(-7, -1)
    win_30d_cust = cust_date.rowsBetween(-30, -1)
    
    df_cust_feat_day = df_cust_trx_day.select(F.col("TX_DATE"),F.col("CUSTOMER_ID"),F.col("NO_TRX"),F.col("TOT_AMOUNT"),
                                  F.lag(F.col("NO_TRX"),1).over(cust_date).as_("CUST_TX_PREV_1"),
                                  F.sum(F.col("NO_TRX")).over(win_7d_cust).as_("CUST_TX_PREV_7"),
                                  F.sum(F.col("NO_TRX")).over(win_30d_cust).as_("CUST_TX_PREV_30"),
                                  F.lag(F.col("TOT_AMOUNT"),1).over(cust_date).as_("CUST_TOT_AMT_PREV_1"),
                                  F.sum(F.col("TOT_AMOUNT")).over(win_7d_cust).as_("CUST_TOT_AMT_PREV_7"),
                                  F.sum(F.col("TOT_AMOUNT")).over(win_30d_cust).as_("CUST_TOT_AMT_PREV_30"))

    ######## return df_cust_feat_day

    # Now we know for each customer and day the number of transactions and amount for previous 1, 7 and 30 days and we add that to our transactions.

    win_cur_date = Window.partition_by(F.col("PARTITION_KEY")).order_by(F.col("TX_DATETIME")).rangeBetween(Window.unboundedPreceding,Window.currentRow)
    
    df_cust_behaviur_feat = df_date_time_feat.join(df_cust_feat_day, (df_date_time_feat["CUSTOMER_ID"] == df_cust_feat_day["CUSTOMER_ID"]) & \
                                                 (F.to_date(df_date_time_feat["TX_DATETIME"]) == df_cust_feat_day["TX_DATE"]))\
            .with_column("PARTITION_KEY", F.concat(df_date_time_feat["CUSTOMER_ID"], F.to_date(df_date_time_feat["TX_DATETIME"])))\
            .with_columns(["CUR_DAY_TRX",
                             "CUR_DAY_AMT"],\
                          [F.count(df_date_time_feat["CUSTOMER_ID"]).over(win_cur_date),
                          F.sum(df_date_time_feat["TX_AMOUNT"]).over(win_cur_date)])\
            .select(df_date_time_feat["TRANSACTION_ID"],
                    df_date_time_feat["CUSTOMER_ID"].as_("CUSTOMER_ID"),
                    df_date_time_feat["TERMINAL_ID"],
                    df_date_time_feat["TX_DATETIME"].as_("TX_DATETIME"),
                    df_date_time_feat["TX_AMOUNT"],
                    df_date_time_feat["TX_TIME_SECONDS"],
                    df_date_time_feat["TX_TIME_DAYS"],
                    df_date_time_feat["TX_FRAUD"],
                    df_date_time_feat["TX_FRAUD_SCENARIO"],
                    df_date_time_feat["TX_DURING_WEEKEND"],
                    df_date_time_feat["TX_DURING_NIGHT"],
                    (zero_if_null(df_cust_feat_day["CUST_TX_PREV_1"]) + F.col("CUR_DAY_TRX")).as_("CUST_CNT_TX_1"),
                    ((zero_if_null(df_cust_feat_day["CUST_TOT_AMT_PREV_1"]) + F.col("CUR_DAY_AMT")) / F.col("CUST_CNT_TX_1")).as_("CUST_AVG_AMOUNT_1"),
                    (zero_if_null(df_cust_feat_day["CUST_TX_PREV_7"]) + F.col("CUR_DAY_TRX")).as_("CUST_CNT_TX_7"),
                    ((zero_if_null(df_cust_feat_day["CUST_TOT_AMT_PREV_7"]) + F.col("CUR_DAY_AMT")) / F.col("CUST_CNT_TX_7")).as_("CUST_AVG_AMOUNT_7"),
                    (zero_if_null(df_cust_feat_day["CUST_TX_PREV_30"]) + F.col("CUR_DAY_TRX")).as_("CUST_CNT_TX_30"),
                    ((zero_if_null(df_cust_feat_day["CUST_TOT_AMT_PREV_30"]) + F.col("CUR_DAY_AMT")) / F.col("CUST_CNT_TX_30")).as_("CUST_AVG_AMOUNT_30"))

    ######## return df_cust_behaviur_feat

    ################################################################################################################
    # Terminal ID Transformations
    ################################################################################################################

    # Since we aggregate by terminal and day and not all terminals have transactions for all dates we cross join our date dataframe with our terminal table so each terminal will have one row for each date
    df_terminals = session.table("TERMINALS").select("TERMINAL_ID")
    df_term_day = df_days.join(df_terminals)
    
    # Aggregate number of transactions and amount by terminal and date, for dates where a terminal do not have any ttransactions we ad a 0
    df_term_trx_by_day = df_cust_trx_fraud.join(df_term_day, (df_cust_trx_fraud["TERMINAL_ID"] == df_term_day["TERMINAL_ID"])\
                        & (F.to_date(df_cust_trx_fraud["TX_DATETIME"]) == df_term_day["TX_DATE"]), "rightouter")\
                    .select(df_term_day["TERMINAL_ID"].as_("TERMINAL_ID"),
                            df_term_day["TX_DATE"], 
                            zero_if_null(df_cust_trx_fraud["TX_FRAUD"]).as_("NB_FRAUD"), 
                            F.when(F.is_null(df_cust_trx_fraud["TX_FRAUD"]), F.lit(0)).otherwise(F.lit(1)).as_("NO_TRX")) \
                    .groupBy(F.col("TERMINAL_ID"), F.col("TX_DATE"))\
                    .agg([F.sum(F.col("NB_FRAUD")).as_("NB_FRAUD"), F.sum(F.col("NO_TRX")).as_("NO_TRX")])
    
    # Aggregate by our windows.
    term_date = Window.partitionBy(F.col("TERMINAL_ID")).orderBy(F.col("TX_DATE"))
    win_delay = term_date.rowsBetween(-7, -1) 
    win_1d_term = term_date.rowsBetween(-8, -1) # We need to add the Delay period to our windows
    win_7d_term = term_date.rowsBetween(-14, -1)
    win_30d_term = term_date.rowsBetween(-37, -1)
    
    df_term_feat_day = df_term_trx_by_day.select(F.col("TX_DATE"),F.col("TERMINAL_ID"),F.col("NO_TRX"), F.col("NB_FRAUD"),
                                  F.sum(F.col("NB_FRAUD")).over(win_delay).as_("NB_FRAUD_DELAY"),
                                  F.sum(F.col("NO_TRX")).over(win_delay).as_("NB_TX_DELAY"),
                                  F.sum(F.col("NO_TRX")).over(win_1d_term).as_("NB_TX_DELAY_WINDOW_1"),
                                  F.sum(F.col("NO_TRX")).over(win_1d_term).as_("NB_TX_DELAY_WINDOW_7"),
                                  F.sum(F.col("NO_TRX")).over(win_30d_term).as_("NB_TX_DELAY_WINDOW_30"),
                                  F.sum(F.col("NB_FRAUD")).over(win_1d_term).as_("NB_FRAUD_DELAY_WINDOW_1"),
                                  F.sum(F.col("NB_FRAUD")).over(win_1d_term).as_("NB_FRAUD_DELAY_WINDOW_7"),
                                  F.sum(F.col("NB_FRAUD")).over(win_30d_term).as_("NB_FRAUD_DELAY_WINDOW_30"))
    
    df_term_behaviur_feat = df_cust_behaviur_feat.join(df_term_feat_day, (df_cust_behaviur_feat["TERMINAL_ID"] == df_term_feat_day["TERMINAL_ID"]) &\
                                                     (F.to_date(df_cust_behaviur_feat["TX_DATETIME"]) == df_term_feat_day["TX_DATE"]))\
                .with_columns(["PARTITION_KEY",
                                 "CUR_DAY_TRX",
                                 "CUR_DAY_FRAUD"],
                             [F.concat(df_cust_behaviur_feat["TERMINAL_ID"], F.to_date(df_cust_behaviur_feat["TX_DATETIME"])),
                                 F.count(df_cust_behaviur_feat["TERMINAL_ID"]).over(win_cur_date),
                                 F.sum(df_cust_behaviur_feat["TX_FRAUD"]).over(win_cur_date)]\
                              )\
                 .with_columns(["NB_TX_DELAY", 
                                  "NB_FRAUD_DELAY",
                                  "NB_TX_DELAY_WINDOW_1",
                                  "NB_FRAUD_DELAY_WINDOW_1",
                                  "NB_TX_DELAY_WINDOW_7",
                                  "NB_FRAUD_DELAY_WINDOW_7",
                                  "NB_TX_DELAY_WINDOW_30",
                                  "NB_FRAUD_DELAY_WINDOW_30"],
                               [df_term_feat_day.col("NB_TX_DELAY") + F.col("CUR_DAY_TRX"),
                                   F.col("NB_FRAUD_DELAY") +  F.col("CUR_DAY_FRAUD"),
                                   F.col("NB_TX_DELAY_WINDOW_1") + F.col("CUR_DAY_TRX"),
                                   F.col("NB_FRAUD_DELAY_WINDOW_1") + F.col("CUR_DAY_FRAUD"),
                                   F.col("NB_TX_DELAY_WINDOW_7") + F.col("CUR_DAY_TRX"),
                                   F.col("NB_FRAUD_DELAY_WINDOW_7") + F.col("CUR_DAY_FRAUD"),
                                   F.col("NB_TX_DELAY_WINDOW_30") + F.col("CUR_DAY_TRX"),
                                   F.col("NB_FRAUD_DELAY_WINDOW_30") + F.col("CUR_DAY_FRAUD")])\
                 .select(df_cust_behaviur_feat["TRANSACTION_ID"], 
                         df_cust_behaviur_feat["TX_DATETIME"].as_("TX_DATETIME"),
                         df_cust_behaviur_feat["CUSTOMER_ID"].as_("CUSTOMER_ID"), 
                         df_cust_behaviur_feat["TERMINAL_ID"].as_("TERMINAL_ID"),
                         df_cust_behaviur_feat["TX_TIME_SECONDS"], 
                         df_cust_behaviur_feat["TX_TIME_DAYS"], 
                         df_cust_behaviur_feat["TX_AMOUNT"], 
                         df_cust_behaviur_feat["TX_FRAUD"], 
                         df_cust_behaviur_feat["TX_FRAUD_SCENARIO"],
                         df_cust_behaviur_feat["TX_DURING_WEEKEND"], 
                         df_cust_behaviur_feat["TX_DURING_NIGHT"],
                         df_cust_behaviur_feat["CUST_AVG_AMOUNT_1"],
                         df_cust_behaviur_feat["CUST_CNT_TX_1"], 
                         df_cust_behaviur_feat["CUST_AVG_AMOUNT_7"],
                         df_cust_behaviur_feat["CUST_CNT_TX_7"], 
                         df_cust_behaviur_feat["CUST_AVG_AMOUNT_30"],
                         df_cust_behaviur_feat["CUST_CNT_TX_30"],
                         (F.col("NB_TX_DELAY_WINDOW_1") - F.col("NB_TX_DELAY")).as_("NB_TX_WINDOW_1"),
                         F.cast(F.iff(F.col("NB_FRAUD_DELAY_WINDOW_1") - F.col("NB_FRAUD_DELAY") > 0, \
                                 (F.col("NB_FRAUD_DELAY_WINDOW_1") - F.col("NB_FRAUD_DELAY")) / F.col("NB_TX_WINDOW_1"), F.lit(0)),T.FloatType()).as_("TERM_RISK_1"),
                         (F.col("NB_TX_DELAY_WINDOW_7") - F.col("NB_TX_DELAY")).as_("NB_TX_WINDOW_7"),
                         F.cast(F.iff(F.col("NB_FRAUD_DELAY_WINDOW_7") - F.col("NB_FRAUD_DELAY") > 0, \
                                 (F.col("NB_FRAUD_DELAY_WINDOW_7") - F.col("NB_FRAUD_DELAY")) / F.col("NB_TX_WINDOW_7"), F.lit(0)),T.FloatType()).as_("TERM_RISK_7"),
                         (F.col("NB_TX_DELAY_WINDOW_30") - F.col("NB_TX_DELAY")).as_("NB_TX_WINDOW_30"),
                         F.cast(F.iff(F.col("NB_FRAUD_DELAY_WINDOW_30") - F.col("NB_FRAUD_DELAY") > 0, \
                                 (F.col("NB_FRAUD_DELAY_WINDOW_30") - F.col("NB_FRAUD_DELAY"))  / F.col("NB_TX_WINDOW_30"), F.lit(0)),T.FloatType()).as_("TERM_RISK_30"))
    ######## return df_term_behaviur_feat

    # Store the result in the table customer_trx_fraud_features
    df_term_behaviur_feat.write.mode("overwrite").save_as_table("customer_trx_fraud_features")

    return df_term_behaviur_feat
    '''